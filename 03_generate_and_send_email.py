import os
import ssl
import smtplib
import pandas as pd
import requests
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

load_dotenv()

RISK_CSV = "out/risk_report.csv"

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_APP_PASSWORD = os.environ.get("SMTP_APP_PASSWORD")

EMAIL_TO = os.environ.get("EMAIL_TO", SMTP_USER)
SENDER_NAME = os.environ.get("SENDER_NAME", "Kevin")

SEND_EMAIL = True
MAX_ROWS_IN_EMAIL = 200

def call_ollama(prompt: str) -> str:
    url = f"{OLLAMA_URL.rstrip('/')}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"num_predict": 300},
    }
    r = requests.post(url, json=payload, timeout=180)
    r.raise_for_status()
    return (r.json().get("response") or "").strip()

def html_escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def build_table_html(df: pd.DataFrame, max_rows: int) -> str:
    cols = [
        "student_key",
        "risk_level",
        "attempts_14d",
        "accuracy_14d",
        "attempts_prev14d",
        "accuracy_prev14d",
        "drop_pct",
        "days_since_last_activity",
        "reason",
    ]
    cols = [c for c in cols if c in df.columns]
    d = df[cols].copy().head(max_rows)

    for c in ["accuracy_14d", "accuracy_prev14d", "drop_pct"]:
        if c in d.columns:
            d[c] = d[c].apply(lambda x: "" if pd.isna(x) else f"{float(x):.2f}")

    if "reason" in d.columns:
        d["reason"] = d["reason"].fillna("").astype(str).str.replace("\n", " ").str.strip()

    ths = "".join([f"<th style='border:1px solid #ddd;padding:8px;text-align:left;background:#f7f7f7'>{html_escape(c)}</th>" for c in cols])

    rows_html = []
    for _, r in d.iterrows():
        tds = ""
        for c in cols:
            val = "" if pd.isna(r[c]) else r[c]
            tds += f"<td style='border:1px solid #ddd;padding:8px;vertical-align:top'>{html_escape(val)}</td>"
        rows_html.append(f"<tr>{tds}</tr>")

    return f"""
    <table style="border-collapse:collapse;width:100%;font-size:13px">
      <thead><tr>{ths}</tr></thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    """.strip()

def attach_file(msg: MIMEMultipart, filepath: str, filename: str):
    part = MIMEBase("application", "octet-stream")
    with open(filepath, "rb") as f:
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

def send_email_html(subject: str, html_body: str, to_addr: str, attachments: list[str] | None = None):
    if not SMTP_USER or not SMTP_APP_PASSWORD:
        raise RuntimeError("SMTP_USER/SMTP_APP_PASSWORD belum diisi di .env")

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = to_addr
    msg["Subject"] = subject

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attachments:
        for path in attachments:
            attach_file(msg, path, os.path.basename(path))

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(SMTP_USER, SMTP_APP_PASSWORD)
        server.sendmail(SMTP_USER, [to_addr], msg.as_string())

def main():
    if not os.path.exists(RISK_CSV):
        raise FileNotFoundError(f"{RISK_CSV} tidak ditemukan. Jalankan script 2 dulu.")

    df = pd.read_csv(RISK_CSV)

    counts = df["risk_level"].value_counts().to_dict()
    high_n = int(counts.get("HIGH", 0))
    med_n = int(counts.get("MED", 0))
    low_n = int(counts.get("LOW", 0))

    df_alert = df[df["risk_level"].isin(["HIGH", "MED"])].copy()

    order = {"HIGH": 0, "MED": 1, "LOW": 2}
    df_alert["_ord"] = df_alert["risk_level"].map(order).fillna(9)
    sort_cols = ["_ord"]
    if "attempts_prev14d" in df_alert.columns:
        sort_cols.append("attempts_prev14d")
    if "attempts_14d" in df_alert.columns:
        sort_cols.append("attempts_14d")
    df_alert = df_alert.sort_values(sort_cols, ascending=[True, False, True]).drop(columns=["_ord"])

    os.makedirs("out", exist_ok=True)
    alert_csv_path = "out/risk_alert_high_med.csv"
    df_alert.to_csv(alert_csv_path, index=False)

    llm_prompt = """
Tulis 2 bagian email dalam Bahasa Indonesia yang natural dan profesional untuk admin sekolah.
JANGAN menulis angka HIGH/MED/LOW, jangan menulis tabel, jangan menulis tanda tangan.

Bagian 1 (Intro, 2-3 kalimat):
- Tujuan: early warning aktivitas & akurasi siswa di platform.

Bagian 2 (Tindakan yang disarankan, max 4 bullet):
- Fokus follow-up siswa risiko HIGH/MED.
- Hindari typo dan jangan campur bahasa Inggris.
""".strip()

    try:
        llm_text = call_ollama(llm_prompt)
    except Exception:
        llm_text = (
            "Halo Admin Sekolah,<br><br>"
            "Berikut kami sampaikan laporan early warning terkait aktivitas pengerjaan dan akurasi jawaban siswa di platform Andalan School. "
            "Tujuannya agar sekolah bisa lebih cepat melakukan follow-up pada siswa yang mulai menunjukkan penurunan tren.<br><br>"
            "<ul>"
            "<li>Prioritaskan follow-up siswa risiko HIGH terlebih dulu, lalu MED.</li>"
            "<li>Koordinasi dengan wali kelas untuk mengecek kendala (akses, motivasi, pemahaman materi).</li>"
            "<li>Jika perlu, berikan pengingat/remedial singkat untuk topik yang paling sering salah.</li>"
            "<li>Lakukan pengecekan ulang 7 hari ke depan untuk melihat apakah tren membaik.</li>"
            "</ul>"
        )

    table_html = build_table_html(df_alert, MAX_ROWS_IN_EMAIL)

    subject = f"[Andalan School] Early Warning (HIGH={high_n}, MED={med_n})"

    html_body = f"""
    <div style="font-family:Arial, sans-serif; font-size:14px; line-height:1.5">
      {llm_text.replace("\n", "<br>")}

      <hr style="border:none;border-top:1px solid #eee;margin:16px 0" />

      <b>Ringkasan:</b>
      <ul>
        <li><b>HIGH:</b> {high_n} siswa</li>
        <li><b>MED:</b> {med_n} siswa</li>
        <li><b>LOW:</b> {low_n} siswa</li>
      </ul>

      <b>Definisi singkat:</b>
      <ul>
        <li><b>HIGH:</b> risiko tinggi, perlu follow-up cepat.</li>
        <li><b>MED:</b> perlu dipantau dan diingatkan.</li>
        <li><b>LOW:</b> normal (tidak perlu detail).</li>
      </ul>

      <b>Daftar siswa HIGH + MED</b> (student_key sebagai identitas sementara):<br><br>
      {table_html}

      <br>
      <i>File CSV (HIGH+MED) terlampir untuk diunduh.</i>
      <br><br>

      Terima kasih.<br><br>
      Salam,<br>
      <b>{html_escape(SENDER_NAME)}</b>
    </div>
    """.strip()

    print("\n===== EMAIL PREVIEW (HTML) =====")
    print("To:", EMAIL_TO)
    print("Subject:", subject)
    print("(HTML body generated; check email inbox for rendering)")

    if not SEND_EMAIL:
        print("[INFO] SEND_EMAIL=False → email tidak dikirim.")
        return

    send_email_html(subject, html_body, EMAIL_TO, attachments=[alert_csv_path])
    print(f"[OK] Email sent to {EMAIL_TO} with attachment: {alert_csv_path}")

if __name__ == "__main__":
    main()