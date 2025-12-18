## UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi 

Nama : Dilla Ayu Puspitasari
NIM : 11221031

## Deskripsi

Project ini adalah implementasi sistem **Distributed Log Aggregator** yang memenuhi kriteria:
- **Idempotency & Strong Deduplication** (Database-level unique constraints)
- **Concurrency Control** (Thread-safe atomic transactions)
- **Fault Tolerance & Persistence** (Docker volumes & PostgreSQL ACID)
- **REST API** (Flask/FastAPI untuk event ingestion)
- **Comprehensive Testing** (15 automated test cases)

---

## Arsitektur

Sistem terdiri dari **3 layanan** dalam Docker Compose:

1. **Publisher** - Client simulator yang mengirim events (simulasi duplikasi 30%)
2. **Aggregator** - REST API server untuk menerima dan memproses events
3. **Database** - PostgreSQL 16 untuk penyimpanan persisten dengan jaminan ACID

Cara Menjalankan (Run)
Prasyarat: Docker & Docker Compose sudah terinstall.

Clone & Masuk ke Folder:
git clone https://github.com/Dillaayh/Pub-Sub-Log-Aggregator-Terdistribusi.git
cd aggregator publisher tests

# Stop all services
docker compose stop

# Stop dan hapus semua (termasuk data)
docker compose down -v
