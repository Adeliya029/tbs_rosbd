$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  SPARK BATCH: MinIO -> PostgreSQL Laptop 3" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Cek containers
$sparkRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_spark_master"
if (-not $sparkRunning) { Write-Host "[!] Spark Master tidak jalan. Jalankan setup dulu" -ForegroundColor Red; exit 1 }

Write-Host "MinIO Local    : rosbd_minio:9000" -ForegroundColor Gray
Write-Host "PostgreSQL L3  : 100.71.251.60:5432" -ForegroundColor Gray
Write-Host ""

# Submit Spark batch job via spark-submit di container
docker exec `
    -e PG_HOST=100.71.251.60 `
    -e PG_PORT=5432 `
    -e PG_DATABASE=seismic_db `
    -e PG_USER=rosbd `
    -e PG_PASSWORD=rosbd123 `
    rosbd_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --name "MinIOToPostgreSQL-Earthquake" `
    --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3" `
    --conf "spark.hadoop.fs.s3a.endpoint=http://rosbd_minio:9000" `
    --conf "spark.hadoop.fs.s3a.access.key=admin" `
    --conf "spark.hadoop.fs.s3a.secret.key=admin12345" `
    --conf "spark.hadoop.fs.s3a.path.style.access=true" `
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" `
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" `
    --conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.disableCertChecking=true" `
    /opt/spark/jobs/minio_to_postgres.py

Write-Host ""
Write-Host "Batch job selesai." -ForegroundColor Yellow
