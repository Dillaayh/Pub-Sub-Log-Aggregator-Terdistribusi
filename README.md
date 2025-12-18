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
git clone https://github.com/syifamaulidaa/SISTER_Pub-Sub-Log-Aggregator-Terdistribusi
cd aggregator publisher tests

# ========================================
# PART 1: SETUP & START SERVICES
# ========================================

# 1. Pindah ke root project
cd "E:\Semester 7\sister\aggregator publisher tests"

# 2. Stop dan clean
docker compose down -v

# 3. Build dan start
docker compose up --build -d

# 4. Wait 20 detik
Start-Sleep -Seconds 20

# 5. Cek status
docker compose ps

# 6. Test health
curl http://localhost:8080/health


# ========================================
# PART 2: MANUAL TESTING
# ========================================

# 7. Publish event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "test",
    "event_id": "evt-001",
    "timestamp": "2025-12-18T12:00:00Z",
    "source": "test",
    "payload": {"data": "hello"}
  }'


# 8. Publish duplicate (test idempotency)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "test",
    "event_id": "evt-001",
    "timestamp": "2025-12-18T12:00:00Z",
    "source": "test",
    "payload": {"data": "hello"}
  }'

# 9. Check stats
curl http://localhost:8080/stats

# Output expected:
# {
#   "received": 2,
#   "unique_processed": 1,
#   "duplicate_dropped": 1,
#   "topics": {"test": 1}
# }

# 10. Get events
curl http://localhost:8080/events

# 11. Check queue status
curl http://localhost:8080/queue/status


# ========================================
# PART 3: AUTOMATED TESTING
# ========================================

# 12. Stop publisher (agar tidak interfere dengan tests)
docker compose stop publisher

# 13. Run automated tests (15 tests)
cd tests
pytest test_api_only.py -v -s

# Expected output:
# ======================== 15 passed in ~18s ========================


# ========================================
# PART 4: MONITORING (OPTIONAL)
# ========================================

# 14. View logs aggregator
docker compose logs aggregator --tail 50

# 15. View logs publisher
docker compose logs publisher --tail 30

# 16. View logs database
docker compose logs db --tail 20

# 17. Follow logs real-time
docker compose logs -f aggregator


# ========================================
# CLEANUP (Jika Selesai)
# ========================================

# Stop all services
docker compose stop

# Stop dan hapus semua (termasuk data)
docker compose down -v
