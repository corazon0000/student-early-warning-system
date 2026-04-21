# Student Early Warning System (EWS) 🚨🎓

Sistem Peringatan Dini (EWS) ini adalah *Data Product* yang dirancang untuk membantu sekolah memantau keaktifan dan akurasi belajar siswa pada platform LMS secara otomatis. Proyek ini merupakan bagian dari tugas magang dalam modul **3.3.5 Early Warning System Aktivitas Siswa**.

## 🧐 Bagaimana Cara Kerjanya?
Sistem mengklasifikasikan siswa ke dalam 3 tingkat risiko:
- **HIGH**: Penurunan aktivitas drastis (>80%) atau akurasi sangat rendah.
- **MED**: Perlu dipantau karena adanya tren penurunan belajar.
- **LOW**: Siswa dengan aktivitas belajar yang normal/stabil.

## 📁 Struktur Repositori & Pipeline
Pipeline data dibagi menjadi 3 tahap utama:

1.  **`01_full_load_pg_to_minio_parquet.py` (Extract)**
    - Menarik data mentah dari PostgreSQL.
    - Menyimpan data ke **MinIO** dalam format **Parquet** (Partitioned by Date) untuk efisiensi penyimpanan dan kecepatan akses.
2.  **`02_compute_risk_duckdb_from_minio.py` (Transform/Analyze)**
    - Menggunakan **DuckDB** untuk memproses data Parquet langsung dari MinIO.
    - Menghitung metrik perbandingan aktivitas (14 hari terakhir vs 14 hari sebelumnya).
    - Menghasilkan file `risk_report.csv`.
3.  **`03_generate_and_send_email.py` (Notify)**
    - Mengambil data risiko kategori HIGH dan MED.
    - Menggunakan **Ollama (Qwen2.5:3b)** untuk merangkum hasil analisis ke dalam bahasa yang mudah dimengerti.
    - Mengirimkan laporan via Email (SMTP) secara otomatis.

## 🛠️ Tech Stack
- **Storage**: MinIO (Object Storage) & Parquet.
- **Query Engine**: DuckDB (Fast Analytical Query).
- **AI/LLM**: Ollama (Self-hosted) for Email Generation.
- **Database**: PostgreSQL.
- **Language**: Python (Pandas, PyArrow, SQLAlchemy).

## 🚀 Cara Menjalankan
1. Pastikan layanan Docker untuk MinIO dan Ollama sudah berjalan.
2. Masukkan kredensial pada file `.env`.
3. Jalankan pipeline secara berurutan:
   ```bash
   python 01_full_load_pg_to_minio_parquet.py
   python 02_compute_risk_duckdb_from_minio.py
   python 03_generate_and_send_email.py
