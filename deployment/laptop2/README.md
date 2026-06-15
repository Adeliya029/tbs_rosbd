# TBS_ROSBD - LAPTOP 2 (Consumer & Spark Processing)

**Tailscale IP:** `100.122.2.11`

---

## Role dalam Pipeline

Laptop 2 bertugas sebagai **processing layer** — menerima data dari Laptop 1 (Kafka), menyimpannya di MinIO, lalu mengirim ke Laptop 3 (PostgreSQL).

```
Laptop 1 (Kafka) ──>  LAPTOP 2 ──> Laptop 3 (PostgreSQL)
  100.76.33.80:9093        │            100.71.251.60:5432
                           ▼
                       ┌─────────┐
                       │  MinIO  │  raw-earthquake/
                       │         │  processed-features/
                       │         │  trained-models/
                       └─────────┘
```

### Yang jalan di Laptop 2:

| Service | Port | Fungsi |
|---------|------|--------|
| **MinIO** | `9000` (API), `9001` (Console) | Object storage: raw, processed, PKL |
| **Spark Master** | `7077`, `8080` (UI) | Koordinator cluster Spark |
| **Spark Worker** | `8081` (UI) | Eksekutor Spark (2 core, 2GB RAM) |
| **Spark Streaming Job** | - | Baca dari Kafka Laptop 1 → simpan ke MinIO |
| **Spark Batch Job** | - | Baca dari MinIO → kirim ke PostgreSQL Laptop 3 |

---

## Prasyarat

1. **Tailscale** sudah install dan konek (`100.122.2.11`)
2. **Docker + Docker Compose** sudah terinstall
3. **Python 3.11+** sudah terinstall
4. **Laptop 1** sudah jalan (Kafka siap)

---

## Cara Setup (Sekali)

Jalankan PowerShell sebagai **Administrator**:

```powershell
.\deployment\laptop2\setup.ps1
```

Script ini akan:
1. Start container: **MinIO** + **Spark Master** + **Spark Worker**
2. Buat 6 bucket di MinIO:
   - `raw-earthquake` — data mentah dari Kafka
   - `historical-earthquake` — data historis
   - `processed-features` — fitur ML
   - `trained-models` — model PKL
   - `spark-checkpoints` — state streaming
   - `analytics` — output analitik
3. Install Python dependencies

---

## Cara Menjalankan

### 1. Jalankan Spark Streaming (Kafka → MinIO)

Script ini submit job Spark yang membaca dari Kafka di **Laptop 1** (`100.76.33.80:9093`) dan menyimpan ke MinIO lokal sebagai Parquet.

```powershell
.\deployment\laptop2\run_streaming.ps1
```

**Cek hasilnya:**
- Buka MinIO Console: `http://localhost:9001` (login: `admin` / `admin12345`)
- Browse bucket `raw-earthquake/earthquake-events/`
- Atau via Spark UI: `http://localhost:8080`

### 2. Jalankan Spark Batch (MinIO → PostgreSQL)

Setelah ada data di MinIO, jalankan job batch untuk mengirim ke PostgreSQL di **Laptop 3** (`100.71.251.60:5432`).

```powershell
.\deployment\laptop2\run_batch.ps1
```

---

## Konfigurasi

### File `.env` (sudah di-set, tidak perlu diubah)

| Variable | Value | Keterangan |
|----------|-------|------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `100.76.33.80:9093` | Kafka Laptop 1 via Tailscale |
| `MINIO_ENDPOINT` | `rosbd_minio:9000` | MinIO lokal (dalam Docker) |
| `MINIO_ENDPOINT_TS` | `100.122.2.11:9000` | MinIO dari luar (via Tailscale) |
| `PG_HOST` | `100.71.251.60` | PostgreSQL Laptop 3 via Tailscale |
| `PG_DATABASE` | `seismic_db` | Database tujuan |
| `SPARK_MASTER` | `spark://spark-master:7077` | Spark master URL |

### Credentials

| Service | Username | Password |
|---------|----------|----------|
| MinIO | `admin` | `admin12345` |
| PostgreSQL | `rosbd` | `rosbd123` |

---

## Monitoring

| Service | URL |
|---------|-----|
| MinIO Console | `http://localhost:9001` |
| Spark Master UI | `http://localhost:8080` |
| Spark Worker UI | `http://localhost:8081` |

---

## Troubleshooting

### Kafka tidak bisa konek ke Laptop 1
```powershell
# Test koneksi via Tailscale
ping 100.76.33.80

# Test Kafka dari container
docker exec rosbd_spark_master sh -c "nc -zv 100.76.33.80 9093"
```

### MinIO bucket tidak muncul
```powershell
# Setup ulang bucket
docker exec rosbd_minio mc alias set local http://localhost:9000 admin admin12345
docker exec rosbd_minio mc mb local/raw-earthquake --ignore-existing
```

### Spark job gagal
Cek log Spark:
```powershell
docker logs rosbd_spark_master --tail 50
docker logs rosbd_spark_worker --tail 50
```

### Data tidak masuk ke PostgreSQL
1. Cek apakah Laptop 3 sudah jalan (PostgreSQL)
2. Cek koneksi: `ping 100.71.251.60`
3. Cek Spark batch log di console

---

## Catatan Penting

- **Pastikan Laptop 1 sudah running** sebelum menjalankan Spark Streaming
- **Pastikan Laptop 3 sudah running** sebelum menjalankan Spark Batch
- Semua koneksi antar laptop menggunakan **Tailscale** (bukan IP publik)
- Job streaming berjalan **continue** sampai di-stop manual (Ctrl+C)
- Job batch berjalan **sekali** setiap kali dijalankan (bisa di-schedule via Prefect di Laptop 3 nanti)
- Data di MinIO tersimpan sebagai **Parquet** (efisien untuk analytics)
