$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Laptop1Dir = $PSScriptRoot

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  TBS_ROSBD - SETUP LAPTOP 1 (PRODUCER)" -ForegroundColor Cyan
Write-Host "  Tailscale IP: 100.76.33.80" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Copy .env ke root project
Write-Host "[1/5] Copy .env configuration..." -ForegroundColor Yellow
Copy-Item -Path "$Laptop1Dir\.env" -Destination "$ProjectRoot\.env" -Force
Write-Host "  OK" -ForegroundColor Green

# Step 2: Start Docker containers
Write-Host "[2/5] Start Zookeeper & Kafka containers..." -ForegroundColor Yellow
Set-Location -Path $Laptop1Dir
docker compose down 2>&1 | Out-Null
docker compose up -d 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Gagal start Docker containers" }
Write-Host "  OK" -ForegroundColor Green

# Step 3: Tunggu Kafka siap
Write-Host "[3/5] Waiting for Kafka to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Cek Kafka ready
$retries = 0
$maxRetries = 10
do {
    $null = docker exec rosbd_kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list 2>&1
    if ($LASTEXITCODE -eq 0) { break }
    $retries++
    Start-Sleep -Seconds 3
    Write-Host "  Menunggu Kafka... ($retries/$maxRetries)" -ForegroundColor Gray
} while ($retries -lt $maxRetries)

if ($LASTEXITCODE -ne 0) { throw "Kafka tidak siap setelah $maxRetries percobaan" }
Write-Host "  Kafka siap!" -ForegroundColor Green

# Step 4: Create Kafka topics
Write-Host "[4/5] Creating Kafka topics..." -ForegroundColor Yellow
docker exec rosbd_kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic earthquake-events --partitions 1 --replication-factor 1 --if-not-exists 2>&1
docker exec rosbd_kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic system-logs --partitions 1 --replication-factor 1 --if-not-exists 2>&1

$topics = docker exec rosbd_kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list 2>&1
Write-Host "  Topics: $topics" -ForegroundColor Green

# Step 5: Install Python dependencies
Write-Host "[5/5] Install Python dependencies..." -ForegroundColor Yellow
Set-Location -Path $ProjectRoot
pip install -r requirements.txt 2>&1
if ($LASTEXITCODE -ne 0) { throw "Gagal install dependencies" }
Write-Host "  OK" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  SETUP LAPTOP 1 SELESAI!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Jalankan producer: .\deployment\laptop1\run.ps1" -ForegroundColor White
Write-Host "  2. Verifikasi Kafka dari Laptop 2: kafka-console-consumer --bootstrap-server 100.76.33.80:9093 --topic earthquake-events" -ForegroundColor White
Write-Host ""

Set-Location -Path $ProjectRoot
