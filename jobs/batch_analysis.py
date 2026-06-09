import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col


# ── Label mappings ────────────────────────────────────────────────────────────

COUNTRY_MAP = {
    "1": "Australia", "2": "Austria", "3": "Belgium",
    "4": "British Virgin Islands", "5": "Cayman Islands",
    "6": "Christmas Island", "7": "Croatia", "8": "Cyprus",
    "9": "Czech Republic", "10": "Denmark", "11": "Estonia",
    "12": "Unidentified", "13": "Faroe Islands", "14": "Finland",
    "15": "France", "16": "Germany", "17": "Greece", "18": "Hungary",
    "19": "Iceland", "20": "India", "21": "Ireland", "22": "Italy",
    "23": "Latvia", "24": "Lithuania", "25": "Luxembourg", "26": "Mexico",
    "27": "Netherlands", "28": "Norway", "29": "Poland", "30": "Portugal",
    "31": "Romania", "32": "Russia", "33": "San Marino", "34": "Slovakia",
    "35": "Slovenia", "36": "Spain", "37": "Sweden", "38": "Switzerland",
    "39": "Ukraine", "40": "United Arab Emirates", "41": "United Kingdom",
    "42": "USA", "43": ".biz", "44": ".com", "45": ".int",
    "46": ".net", "47": ".org",
}

MAIN_CATEGORY_MAP = {
    "1": "Trousers",
    "2": "Skirts",
    "3": "Blouses",
    "4": "Sale",
}

COLOUR_MAP = {
    "1": "Beige", "2": "Black", "3": "Blue", "4": "Brown",
    "5": "Burgundy", "6": "Gray", "7": "Green", "8": "Navy Blue",
    "9": "Multi Color", "10": "Olive", "11": "Pink", "12": "Red",
    "13": "Violet", "14": "White",
}

LOCATION_MAP = {
    "1": "Top Left", "2": "Top Middle", "3": "Top Right",
    "4": "Bottom Left", "5": "Bottom Middle", "6": "Bottom Right",
}

MODEL_PHOTOGRAPHY_MAP = {
    "1": "En Face",
    "2": "Profile",
}

PRICE_LEVEL_MAP = {
    "1": "Above Average",
    "2": "Below Average",
}


def _map_col(column, mapping, default="Unknown"):
    """Build a chained when().otherwise() expression from a dict mapping."""
    expr = None
    for k, v in mapping.items():
        cond = col(column).cast("string") == k
        expr = when(cond, v) if expr is None else expr.when(cond, v)
    return expr.otherwise(default) if expr is not None else F.lit(default)


def apply_labels(df):
    """Add human-readable label columns next to every coded column."""
    return (
        df
        .withColumn("country_name",           _map_col("country",           COUNTRY_MAP))
        .withColumn("main_category_name",      _map_col("main_category",     MAIN_CATEGORY_MAP))
        .withColumn("colour_name",             _map_col("colour",            COLOUR_MAP))
        .withColumn("location_name",           _map_col("location",          LOCATION_MAP))
        .withColumn("model_photography_name",  _map_col("model photography", MODEL_PHOTOGRAPHY_MAP))
        .withColumn("price_level_name",        _map_col("price_level",       PRICE_LEVEL_MAP))
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = (
        SparkSession.builder
        .appName("clickstream-batch-analysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path  = "hdfs://namenode:9000/alp/input/e-shop clothing 2008.csv"
    output_path = "hdfs://namenode:9000/alp/output/batch_analysis"

    # ── Load & rename columns ─────────────────────────────────────────────────
    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(input_path)
    )

    clickstream_df = (
        df
        .withColumnRenamed("session ID",              "session_id")
        .withColumnRenamed("page 1 (main category)",  "main_category")
        .withColumnRenamed("page 2 (clothing model)", "clothing_model")
        .withColumnRenamed("price 2",                 "price_level")
        .withColumnRenamed("order",                   "order_in_session")
    )

    # Apply human-readable labels
    labeled_df = apply_labels(clickstream_df)

    # ── Aggregations ──────────────────────────────────────────────────────────

    summary_df = labeled_df.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("unique_sessions"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.min("price").alias("minimum_price"),
        F.max("price").alias("maximum_price"),
    )

    # Top categories — show name instead of numeric code
    top_categories_df = (
        labeled_df
        .groupBy("main_category", "main_category_name")
        .count()
        .orderBy(F.desc("count"))
    )

    top_models_df = (
        labeled_df
        .groupBy("clothing_model")
        .count()
        .orderBy(F.desc("count"))
    )

    # Country distribution — show country name
    country_distribution_df = (
        labeled_df
        .groupBy("country", "country_name")
        .count()
        .orderBy(F.desc("count"))
    )

    # Average price by category — show name
    avg_price_by_category_df = (
        labeled_df
        .groupBy("main_category", "main_category_name")
        .agg(
            F.round(F.avg("price"), 2).alias("average_price"),
            F.count("*").alias("total_views"),
        )
        .orderBy("main_category")
    )

    # Page distribution (page numbers are self-explanatory, no mapping needed)
    page_distribution_df = (
        labeled_df
        .groupBy("page")
        .count()
        .orderBy("page")
    )

    # Colour distribution — show colour name
    colour_distribution_df = (
        labeled_df
        .groupBy("colour", "colour_name")
        .count()
        .orderBy(F.desc("count"))
    )

    # Location distribution — show location name
    location_distribution_df = (
        labeled_df
        .groupBy("location", "location_name")
        .count()
        .orderBy("location")
    )

    # Price level distribution — show label
    price_level_distribution_df = (
        labeled_df
        .groupBy("price_level", "price_level_name")
        .count()
        .orderBy("price_level")
    )

    # ── Console output ────────────────────────────────────────────────────────

    print("\n=== Dataset Summary ===")
    summary_df.show(truncate=False)

    print("\n=== Top Main Categories ===")
    top_categories_df.show(truncate=False)

    print("\n=== Top Clothing Models ===")
    top_models_df.show(20, truncate=False)

    print("\n=== Country Distribution ===")
    country_distribution_df.show(20, truncate=False)

    print("\n=== Average Price by Category ===")
    avg_price_by_category_df.show(truncate=False)

    print("\n=== Page Distribution ===")
    page_distribution_df.show(truncate=False)

    print("\n=== Colour Distribution ===")
    colour_distribution_df.show(truncate=False)

    print("\n=== Location Distribution ===")
    location_distribution_df.show(truncate=False)

    print("\n=== Price Level Distribution ===")
    price_level_distribution_df.show(truncate=False)

    # ── Write to HDFS ─────────────────────────────────────────────────────────

    summary_df               .write.mode("overwrite").json(f"{output_path}/summary")
    top_categories_df        .write.mode("overwrite").json(f"{output_path}/top_categories")
    top_models_df            .write.mode("overwrite").json(f"{output_path}/top_models")
    country_distribution_df  .write.mode("overwrite").json(f"{output_path}/country_distribution")
    avg_price_by_category_df .write.mode("overwrite").json(f"{output_path}/avg_price_by_category")
    page_distribution_df     .write.mode("overwrite").json(f"{output_path}/page_distribution")
    colour_distribution_df   .write.mode("overwrite").json(f"{output_path}/colour_distribution")
    location_distribution_df .write.mode("overwrite").json(f"{output_path}/location_distribution")
    price_level_distribution_df.write.mode("overwrite").json(f"{output_path}/price_level_distribution")

    print(f"\nBatch analysis output saved to: {output_path}")

    # ── Write dashboard JSON ──────────────────────────────────────────────────

    dashboard_dir = Path(os.getenv("DASHBOARD_DIR", "/opt/alp/dashboard_data"))
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    batch_dashboard_payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "summary": summary_df.collect()[0].asDict(recursive=True),
        "top_categories": [
            row.asDict(recursive=True)
            for row in top_categories_df.collect()
        ],
        "top_models": [
            row.asDict(recursive=True)
            for row in top_models_df.limit(20).collect()
        ],
        "country_distribution": [
            row.asDict(recursive=True)
            for row in country_distribution_df.limit(20).collect()
        ],
        "avg_price_by_category": [
            row.asDict(recursive=True)
            for row in avg_price_by_category_df.collect()
        ],
        "page_distribution": [
            row.asDict(recursive=True)
            for row in page_distribution_df.collect()
        ],
        "colour_distribution": [
            row.asDict(recursive=True)
            for row in colour_distribution_df.collect()
        ],
        "location_distribution": [
            row.asDict(recursive=True)
            for row in location_distribution_df.collect()
        ],
        "price_level_distribution": [
            row.asDict(recursive=True)
            for row in price_level_distribution_df.collect()
        ],
    }

    batch_dashboard_path      = dashboard_dir / "batch_analysis.json"
    temp_batch_dashboard_path = dashboard_dir / "batch_analysis.json.tmp"

    temp_batch_dashboard_path.write_text(
        json.dumps(batch_dashboard_payload, indent=2),
        encoding="utf-8",
    )
    temp_batch_dashboard_path.replace(batch_dashboard_path)

    print(f"Batch dashboard data saved to: {batch_dashboard_path}")

    spark.stop()


if __name__ == "__main__":
    main()