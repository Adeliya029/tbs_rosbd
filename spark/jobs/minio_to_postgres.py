"""
Spark Job: Baca data dari MinIO dan sink ke PostgreSQL
Alur: MinIO (raw) → Spark Batch → PostgreSQL
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Konfigurasi
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "rosbd_minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin12345")

BUCKET_RAW = "raw-earthquake"

# PostgreSQL configuration
PG_HOST = "rosbd_postgres"
PG_PORT = "5432"
PG_DATABASE = "seismic_db"
PG_USER = "rosbd"
PG_PASSWORD = "rosbd123"

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def main():
    """Main Spark batch job."""
    
    # Buat Spark session
    spark = SparkSession.builder \
        .appName("MinIOToPostgreSQL-Earthquake") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.3") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 50)
    print("Spark Job: MinIO → PostgreSQL Earthquake Pipeline")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Bucket: {BUCKET_RAW}")
    print(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    print("=" * 50)
    
    try:
        # Baca data dari MinIO (asumsi format Parquet dari streaming job)
        minio_path = f"s3a://{BUCKET_RAW}/earthquake-events/"
        
        print(f"Membaca data dari: {minio_path}")
        
        # Coba baca data, jika tidak ada file, tangani gracefully
        try:
            df = spark.read.parquet(minio_path)
            record_count = df.count()
            
            if record_count == 0:
                print("Tidak ada data di MinIO")
                return
                
            print(f"Ditemukan {record_count} records di MinIO")
            
            # Tampilkan sample
            print("\nSample data (5 records pertama):")
            df.select("event_id", "datetime", "magnitude", "latitude", "longitude", "source") \
              .show(5, truncate=False)
            
            # Transformasi: ubah datetime string ke timestamp
            # Format BMKG: "2026-06-07T13:06:08+00:00" atau "07 Jun 2026 13:06:08 WIB"
            df_transformed = df.withColumn(
                "event_time",
                to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss")
            ).withColumnRenamed("datetime", "original_datetime")
            
            # Pilih kolom sesuai dengan tabel PostgreSQL
            df_for_pg = df_transformed.select(
                col("event_id"),
                col("event_time"),
                col("latitude"),
                col("longitude"),
                col("magnitude"),
                col("depth"),
                col("region"),
                col("felt_intensity"),
                col("source"),
                col("ingested_at")
            )
            
            # Write ke PostgreSQL
            print(f"\nMenulis {record_count} records ke PostgreSQL...")
            
            df_for_pg.write \
                .mode("append") \
                .jdbc(
                    url=PG_JDBC_URL,
                    table="earthquakes",
                    properties=PG_PROPERTIES
                )
            
            print("✅ Data berhasil ditulis ke PostgreSQL")
            
            # Cek total records di PostgreSQL setelah insert
            print("\nMengecek data di PostgreSQL...")
            
            # Baca dari PostgreSQL untuk verifikasi
            pg_df = spark.read \
                .jdbc(
                    url=PG_JDBC_URL,
                    table="earthquakes",
                    properties=PG_PROPERTIES
                )
            
            total_in_pg = pg_df.count()
            print(f"Total records di PostgreSQL: {total_in_pg}")
            
            # Tampilkan sample dari PostgreSQL
            print("\nSample dari PostgreSQL (5 records terbaru):")
            pg_df.select("event_id", "event_time", "magnitude", "region", "source") \
                 .orderBy(col("event_time").desc()) \
                 .show(5, truncate=False)
                 
        except Exception as e:
            print(f"❌ Error saat membaca data dari MinIO: {e}")
            print("Mungkin belum ada data dari streaming job.")
            print("Jalankan Spark streaming job terlebih dahulu.")
            return
            
    except Exception as e:
        print(f"❌ Error dalam job: {e}")
        raise
        
    finally:
        spark.stop()
        print("\nSpark session dihentikan.")

if __name__ == "__main__":
    main()