import os
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MINIO_BUCKET = os.environ["MINIO_BUCKET"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

PARQUET_GLOB = f"s3://{MINIO_BUCKET}/bronze/public.kp_ptn_practice_user_answer/dt=*/part-*.parquet"

WINDOW_DAYS = 14

MIN_ATTEMPTS_FOR_ACCURACY = 15
LOW_ACCURACY_THRESHOLD = 0.40
DROP_PCT_THRESHOLD_MED = 0.60
DROP_PCT_THRESHOLD_HIGH = 0.80

OUT_DIR = "out"
OUT_CSV = os.path.join(OUT_DIR, "risk_report.csv")

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    endpoint_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")

    con.execute(f"SET s3_endpoint='{endpoint_host}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    con.execute(f"""
        CREATE OR REPLACE VIEW answers AS
        SELECT
            practice_user_history_id AS student_key,
            created_at::TIMESTAMP AS created_at,
            is_correct,
            deleted_at
        FROM read_parquet('{PARQUET_GLOB}', union_by_name=true);
    """)

    as_of = con.execute("""
        SELECT MAX(created_at) AS as_of
        FROM answers
        WHERE deleted_at IS NULL
    """).fetchone()[0]

    if as_of is None:
        raise RuntimeError("Tidak ada data (as_of NULL). Cek parquet path / bucket.")

    print(f"[INFO] as_of (max created_at) = {as_of}")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW base AS
        SELECT
            student_key,
            created_at,
            is_correct
        FROM answers
        WHERE deleted_at IS NULL
          AND created_at >= TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS*2} days'
          AND created_at <= TIMESTAMP '{as_of}';
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW feat AS
        SELECT
            student_key,

            COUNT(*) FILTER (
                WHERE created_at >  TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS} days'
                  AND created_at <= TIMESTAMP '{as_of}'
            ) AS attempts_14d,

            COUNT(DISTINCT DATE(created_at)) FILTER (
                WHERE created_at >  TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS} days'
                  AND created_at <= TIMESTAMP '{as_of}'
            ) AS active_days_14d,

            AVG(CASE
                WHEN created_at >  TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS} days'
                 AND created_at <= TIMESTAMP '{as_of}'
                 AND is_correct IS NOT NULL
                THEN CASE WHEN is_correct THEN 1.0 ELSE 0.0 END
                ELSE NULL
            END) AS accuracy_14d,

            COUNT(*) FILTER (
                WHERE created_at >  TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS*2} days'
                  AND created_at <= TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS} days'
            ) AS attempts_prev14d,

            AVG(CASE
                WHEN created_at >  TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS*2} days'
                 AND created_at <= TIMESTAMP '{as_of}' - INTERVAL '{WINDOW_DAYS} days'
                 AND is_correct IS NOT NULL
                THEN CASE WHEN is_correct THEN 1.0 ELSE 0.0 END
                ELSE NULL
            END) AS accuracy_prev14d,

            MAX(created_at) AS last_activity
        FROM base
        GROUP BY student_key;
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW risk AS
        SELECT
            student_key,
            attempts_14d,
            active_days_14d,
            accuracy_14d,
            attempts_prev14d,
            accuracy_prev14d,
            last_activity,

            CASE
              WHEN attempts_prev14d > 0 THEN
                (attempts_prev14d - attempts_14d) * 1.0 / attempts_prev14d
              ELSE NULL
            END AS drop_pct,

            DATE_DIFF('day', DATE(last_activity), DATE(TIMESTAMP '{as_of}')) AS days_since_last_activity,

            CASE
              WHEN
                (attempts_14d = 0 AND attempts_prev14d >= 10)
                OR
                (attempts_14d >= {MIN_ATTEMPTS_FOR_ACCURACY} AND accuracy_14d IS NOT NULL AND accuracy_14d < {LOW_ACCURACY_THRESHOLD})
                OR
                (attempts_prev14d >= 10 AND drop_pct IS NOT NULL AND drop_pct >= {DROP_PCT_THRESHOLD_HIGH})
              THEN 'HIGH'

              WHEN
                (attempts_14d <= 3 AND attempts_prev14d >= 5)
                OR
                (attempts_prev14d >= 5 AND drop_pct IS NOT NULL AND drop_pct >= {DROP_PCT_THRESHOLD_MED})
                OR
                (attempts_14d >= {MIN_ATTEMPTS_FOR_ACCURACY} AND accuracy_14d IS NOT NULL AND accuracy_14d >= {LOW_ACCURACY_THRESHOLD} AND accuracy_14d < 0.55)
              THEN 'MED'

              ELSE 'LOW'
            END AS risk_level,

            TRIM(BOTH ';' FROM
              CONCAT(
                CASE WHEN attempts_14d = 0 AND attempts_prev14d >= 10 THEN 'No activity in last 14d but active in previous 14d; ' ELSE '' END,
                CASE WHEN attempts_prev14d >= 10 AND drop_pct IS NOT NULL AND drop_pct >= {DROP_PCT_THRESHOLD_HIGH} THEN 'Activity dropped >= 80% vs previous 14d; ' ELSE '' END,
                CASE WHEN attempts_prev14d >= 5  AND drop_pct IS NOT NULL AND drop_pct >= {DROP_PCT_THRESHOLD_MED} THEN 'Activity dropped >= 60% vs previous 14d; ' ELSE '' END,
                CASE WHEN attempts_14d >= {MIN_ATTEMPTS_FOR_ACCURACY} AND accuracy_14d IS NOT NULL AND accuracy_14d < {LOW_ACCURACY_THRESHOLD} THEN 'Low accuracy (<40%) with sufficient attempts; ' ELSE '' END,
                CASE WHEN attempts_14d >= {MIN_ATTEMPTS_FOR_ACCURACY} AND accuracy_14d IS NOT NULL AND accuracy_14d >= {LOW_ACCURACY_THRESHOLD} AND accuracy_14d < 0.55 THEN 'Moderate accuracy (40–55%) with sufficient attempts; ' ELSE '' END,
                CASE WHEN attempts_14d <= 3 AND attempts_prev14d >= 5 THEN 'Very low attempts in last 14d; ' ELSE '' END
              )
            ) AS reason
        FROM feat;
    """)

    df = con.execute("""
        SELECT *
        FROM risk
        ORDER BY
          CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
          attempts_prev14d DESC,
          attempts_14d ASC
    """).df()

    df.to_csv(OUT_CSV, index=False)
    print(f"[OK] risk report saved: {OUT_CSV}")

    print("\n=== TOP 20 RISK STUDENTS ===")
    print(df.head(20).to_string(index=False))

if __name__ == "__main__":
    main()