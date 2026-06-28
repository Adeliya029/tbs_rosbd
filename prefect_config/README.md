# Setup Guide — 3 Laptop Configuration
## Real-Time Seismic Intelligence System · ROSBD REV 3.0

```
Laptop 1  →  Prefect Worker (scraping-pool)
Laptop 2  →  Prefect Worker (training-pool)
Laptop 3  →  Prefect Server + MinIO + PostgreSQL
```

---

## Prasyarat — Semua Laptop

- Docker Desktop terinstall dan running
- WSL2 aktif (Windows)
- Semua laptop terhubung ke **WiFi/LAN yang sama**
- Port tidak diblokir Windows Firewall (lihat bagian Firewall di bawah)

---

## Step 1 — Cek IP Laptop 3

Di Laptop 3, buka Command Prompt:
```
ipconfig
```
Cari baris **IPv4 Address** di adapter WiFi/LAN yang aktif.
Contoh: `192.168.1.10`

**Catat IP ini** — akan dipakai di .env laptop 1 dan 2.

---

## Step 2 — Setup Laptop 3 (Server) — DULUAN

```bash
# 1. Masuk ke folder laptop3
cd laptop3

# 2. Copy dan edit .env
cp .env.example .env
# (tidak perlu edit apapun kalau pakai default)

# 3. Jalankan semua service
docker compose up -d

# 4. Cek semua service jalan
docker compose ps

# 5. Verifikasi Prefect Server bisa diakses
curl http://localhost:4200/api/health
# Harusnya: {"status":"healthy"}

# 6. Verifikasi MinIO bisa diakses
curl http://localhost:9000/minio/health/live
# Harusnya: status 200
```

**Verifikasi dari laptop lain** (ganti IP):
```bash
curl http://192.168.1.10:4200/api/health
curl http://192.168.1.10:9000/minio/health/live
```

---

## Step 3 — Setup Laptop 1 (Worker Scraping)

```bash
# 1. Masuk ke folder laptop1
cd laptop1

# 2. Copy dan edit .env
cp .env.example .env

# 3. WAJIB: isi IP laptop 3
# Buka .env, ubah baris:
# LAPTOP3_IP=192.168.1.10  ← ganti dengan IP aktual laptop 3

# 4. Jalankan worker
docker compose up -d

# 5. Cek worker muncul di Prefect UI
# Buka browser: http://192.168.1.10:4200
# → Work Pools → scraping-pool → harus ada worker online
```

### Register flows ke server (sekali saja):
```bash
docker compose --profile deploy run --rm prefect-deploy
```

### Jalankan historical scraping (manual, sekali saat setup):
```bash
docker compose exec prefect-worker-scraping \
  python -c "from flows.historical_scraping import historical_scraping_flow; historical_scraping_flow()"
```

---

## Step 4 — Setup Laptop 2 (Worker Training)

```bash
# 1. Masuk ke folder laptop2
cd laptop2

# 2. Copy dan edit .env
cp .env.example .env

# 3. WAJIB: isi IP laptop 3 (SAMA dengan yang di laptop 1)
# LAPTOP3_IP=192.168.1.10

# 4. Jalankan worker
docker compose up -d

# 5. Cek worker muncul di Prefect UI
# Buka browser: http://192.168.1.10:4200
# → Work Pools → training-pool → harus ada worker online
```

### Register flows ke server (sekali saja):
```bash
docker compose --profile deploy run --rm prefect-deploy
```

---

## Urutan Startup yang Benar

```
1. Laptop 3 up  →  tunggu semua service healthy
2. Laptop 1 up  →  worker scraping mulai poll ke server laptop 3
3. Laptop 2 up  →  worker training mulai poll ke server laptop 3
4. Laptop 1     →  jalankan historical scraping (manual)
5. Laptop 2     →  training otomatis terjadwal setiap Senin 02.00
                   atau trigger manual dari Prefect UI
```

---

## Akses UI Semua Service

| Service | URL | Laptop |
|---|---|---|
| Prefect UI | http://IP_LAPTOP3:4200 | Semua laptop |
| MinIO Console | http://IP_LAPTOP3:9001 | Semua laptop |
| Streamlit | http://IP_LAPTOP_STREAM:8501 | Laptop yg run Streamlit |
| FastAPI docs | http://IP_LAPTOP_API:8000/docs | Laptop yg run FastAPI |
| Spark UI | http://IP_LAPTOP_SPARK:8080 | Laptop yg run Spark |

---

## Setup Windows Firewall (WAJIB di Laptop 3)

Port yang harus dibuka di Laptop 3 agar bisa diakses laptop lain:

```
Buka: Windows Defender Firewall
→ Advanced Settings
→ Inbound Rules
→ New Rule
→ Port
→ TCP
→ Specific local ports: 4200, 9000, 9001, 5432
→ Allow the connection
→ Centang: Domain, Private, Public
→ Name: Seismic Project Ports
```

Atau via PowerShell (run as Administrator):
```powershell
New-NetFirewallRule -DisplayName "Seismic Prefect" -Direction Inbound -Protocol TCP -LocalPort 4200 -Action Allow
New-NetFirewallRule -DisplayName "Seismic MinIO" -Direction Inbound -Protocol TCP -LocalPort 9000-9001 -Action Allow
New-NetFirewallRule -DisplayName "Seismic PostgreSQL" -Direction Inbound -Protocol TCP -LocalPort 5432 -Action Allow
```

---

## Troubleshooting

### Worker tidak muncul di Prefect UI
```bash
# Cek apakah PREFECT_API_URL benar
docker compose exec prefect-worker-scraping \
  curl http://IP_LAPTOP3:4200/api/health

# Cek logs worker
docker compose logs prefect-worker-scraping
```

### MinIO tidak bisa diakses dari laptop lain
```bash
# Test dari laptop 1/2
curl http://IP_LAPTOP3:9000/minio/health/live

# Kalau timeout → firewall belum dibuka di laptop 3
# Kalau connection refused → MinIO container belum jalan
```

### Flow gagal karena tidak bisa akses MinIO
```bash
# Cek environment variable di worker
docker compose exec prefect-worker-scraping env | grep MINIO

# Pastikan MINIO_ENDPOINT menggunakan IP laptop 3, bukan localhost
# SALAH:  MINIO_ENDPOINT=http://localhost:9000
# BENAR:  MINIO_ENDPOINT=http://192.168.1.10:9000
```

### IP Laptop 3 berubah (DHCP)
Set IP statis di Windows:
```
Settings → Network → WiFi → Properties
→ IP settings: Manual
→ IPv4: isi IP yang tadi (misal 192.168.1.10)
→ Subnet: 255.255.255.0
→ Gateway: 192.168.1.1 (IP router)
```
