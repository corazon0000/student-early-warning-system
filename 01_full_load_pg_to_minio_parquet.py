import os
from datetime import datetime, timedelta, date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

load_dotenv()

TABLE_SCHEMA = "public"
TABLE_NAME = "kp_ptn_practice_user_answer"

DATE_COL = "created_at"
DELETED_COL = "deleted_at"

S3_PREFIX = f"bronze/{TABLE_SCHEMA}.{TABLE_NAME}"

CHUNK_SIZE = 20000

PURGE_PREFIX_BEFORE_LOAD = False

DELETE_OBJECT_BEFORE_WRITE = True

def get_pg_engine():
    pg_host = os.environ["PG_HOST"]
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_db = os.environ["PG_DB"]
    pg_user = os.environ["PG_USER"]
    pg_pwd = os.environ["PG_PASSWORD"]
    url = f"postgresql+psycopg2://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(url)

def get_s3_fs():
    endpoint = os.environ["MINIO_ENDPOINT"].rstrip("/")
    key = os.environ["MINIO_ACCESS_KEY"]
    secret = os.environ["MINIO_SECRET_KEY"]
    return s3fs.S3FileSystem(
        key=key,
        secret=secret,
        client_kwargs={"endpoint_url": endpoint, "region_name": "us-east-1"},
    )

def s3_key(bucket: str, key: str) -> str:
    return f"{bucket}/{key}"

def daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def purge_prefix(fs: s3fs.S3FileSystem, bucket: str, prefix: str):
    full_prefix = f"{bucket}/{prefix}"
    if not fs.exists(full_prefix):
        return
    paths = fs.find(full_prefix)
    if paths:
        fs.rm(paths, recursive=True)

def main():
    bucket = os.environ["MINIO_BUCKET"]
    engine = get_pg_engine()
    fs = get_s3_fs()

    try:
        fs.ls("")
    except Exception as e:
        raise RuntimeError(f"MinIO auth/endpoint failed: {e}") from e

    if not fs.exists(bucket):
        raise RuntimeError(f"Bucket '{bucket}' tidak ditemukan. Buat dulu di MinIO Console (localhost:9001).")

    full_table = f"{TABLE_SCHEMA}.{TABLE_NAME}"

    where_valid = ""
    if DELETED_COL:
        where_valid = f"WHERE {DELETED_COL} IS NULL"

    q_minmax = f"""
        SELECT MIN({DATE_COL}) AS min_dt, MAX({DATE_COL}) AS max_dt, COUNT(*) AS total_rows
        FROM {full_table}
        {where_valid};
    """
    with engine.connect() as conn:
        row = conn.execute(text(q_minmax)).mappings().one()

    min_dt = row["min_dt"]
    max_dt = row["max_dt"]
    total_rows = row["total_rows"]

    if not max_dt or not min_dt:
        raise RuntimeError("MIN/MAX created_at kosong. Cek tabel & kolom created_at.")

    start_date = min_dt.date()
    end_date = max_dt.date()

    print(f"[INFO] {full_table} total_rows={total_rows}")
    print(f"[INFO] FULL LOAD range={start_date} .. {end_date}")

    if PURGE_PREFIX_BEFORE_LOAD:
        print(f"[WARN] PURGE enabled. Removing s3://{bucket}/{S3_PREFIX}/ ...")
        purge_prefix(fs, bucket, S3_PREFIX)
        print("[OK] Purge done.")

    for d in daterange(start_date, end_date):
        day_start = datetime(d.year, d.month, d.day, 0, 0, 0)
        day_end = day_start + timedelta(days=1)

        filters = [f"{DATE_COL} >= :day_start", f"{DATE_COL} < :day_end"]
        if DELETED_COL:
            filters.append(f"{DELETED_COL} IS NULL")
        where_clause = "WHERE " + " AND ".join(filters)

        q_count = f"SELECT COUNT(*) FROM {full_table} {where_clause};"
        with engine.connect() as conn:
            c = conn.execute(text(q_count), {"day_start": day_start, "day_end": day_end}).scalar()

        if c == 0:
            continue

        key_prefix = f"{S3_PREFIX}/dt={d.isoformat()}"
        out_key = f"{key_prefix}/part-000.parquet"
        out_path = s3_key(bucket, out_key)

        print(f"[INFO] dt={d} rows={c} -> writing {out_path}")

        q_data = f"SELECT * FROM {full_table} {where_clause} ORDER BY {DATE_COL} ASC;"
        chunks = []
        for df in pd.read_sql_query(
            sql=text(q_data),
            con=engine,
            params={"day_start": day_start, "day_end": day_end},
            chunksize=CHUNK_SIZE,
        ):
            chunks.append(df)

        df_all = pd.concat(chunks, ignore_index=True)
        table = pa.Table.from_pandas(df_all, preserve_index=False)

        if DELETE_OBJECT_BEFORE_WRITE and fs.exists(out_path):
            fs.rm(out_path)

        with fs.open(out_path, "wb") as f_out:
            pq.write_table(table, f_out, compression="snappy")

        print(f"[OK] wrote s3://{out_path}")

    print("[DONE] FULL LOAD finished.")

if __name__ == "__main__":
    main()