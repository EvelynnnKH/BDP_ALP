import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def build_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("country", StringType(), True),
        StructField("main_category", StringType(), True),
        StructField("clothing_model", StringType(), True),
        StructField("colour", StringType(), True),
        StructField("location", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("price_level", StringType(), True),
        StructField("page", StringType(), True),
        StructField("order_in_session", IntegerType(), True),
    ])


def write_dashboard_batch(batch_df, batch_id):
    dashboard_dir = Path(os.getenv("DASHBOARD_DIR", "/opt/alp/dashboard_data"))
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    # Karena kita mengirimkan raw data ke sini, kita lakukan caching agar kalkulasi cepat
    batch_df.cache()

    # 1. Agregasi Utama: Kategori (Untuk Bar Chart Utama & Live Table)
    categories_df = (
        batch_df.groupBy("main_category")
        .count()
        .orderBy("main_category")
    )
    # Diubah ke list of dict sesuai kebutuhan kode lami kamu
    rows = [row.asDict(recursive=True) for row in categories_df.collect()]

    # 2. Agregasi Tambahan A: Warna Produk (Untuk Dashboard Overview Right Column)
    color_df = (
        batch_df.groupBy("colour")
        .count()
        .orderBy(F.desc("count"))
    )
    color_rows = [list(row.values()) for row in [r.asDict() for r in color_df.collect()]]

    # 3. Agregasi Tambahan B: Top 10 Produk (Untuk Analytics Page)
    product_df = (
        batch_df.groupBy("clothing_model")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
    )
    product_rows = [list(row.values()) for row in [r.asDict() for r in product_df.collect()]]

    # 4. Agregasi Tambahan C: Analisis Sensitivitas Harga (Untuk Analytics Page)
    price_df = (
        batch_df.groupBy("main_category")
        .agg(
            F.avg("price").alias("avg_price"),
            F.count("*").alias("total_clicks")
        )
    )
    price_analytics = [list(row.values()) for row in [r.asDict() for r in price_df.collect()]]

    # Susun Payload untuk latest_snapshot.json
    payload = {
        "batch_id": batch_id,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rows": rows,
        "color_rows": color_rows,            # Data Baru 🚀
        "product_rows": product_rows,        # Data Baru 🚀
        "price_analytics": price_analytics   # Data Baru 🚀
    }

    latest_snapshot = dashboard_dir / "latest_snapshot.json"
    temp_snapshot = dashboard_dir / "latest_snapshot.json.tmp"

    temp_snapshot.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temp_snapshot.replace(latest_snapshot)

    total_events = sum(int(r.get("count", 0)) for r in rows)

    history_record = {
        "batch_id": batch_id,
        "updated_at": payload["updated_at"],
        "total_events": total_events,
        "category_totals": {
            str(r["main_category"]): int(r["count"])
            for r in rows
            if r.get("main_category") is not None
        },
    }

    history_file = dashboard_dir / "history.jsonl"
    with history_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(history_record) + "\n")

    print()
    print("-" * 60)
    print(f"Batch: {batch_id} | Total events processed in this batch: {total_events}")
    print("-" * 60)
    categories_df.show(truncate=False)
    
    # Hapus cache setelah selesai diproses
    batch_df.unpersist()


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "clickstream-events")
    checkpoint_dir = os.getenv(
        "CHECKPOINT_DIR",
        "/opt/alp/checkpoints/streaming_aggregation"
    )

    spark = (
        SparkSession.builder
        .appName("clickstream-fashion-aggregation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS raw_json")
        .select(F.from_json("raw_json", build_schema()).alias("event"))
        .select("event.*")
    )

    # PERUBAHAN UTAMA: Kita kirim parsed_df (data mentah yang sudah ada skemanya)
    # langsung ke foreachBatch, bukan mengirim data yang sudah terlanjur di-groupBy.
    query = (
        parsed_df.writeStream
        .outputMode("update") # Menggunakan update mode karena pemrosesan agregasi dipindah ke dalam batch function
        .foreachBatch(write_dashboard_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()