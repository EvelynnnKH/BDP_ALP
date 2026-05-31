from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("clickstream-batch-analysis")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    input_path = "hdfs://namenode:9000/alp/input/eshop_clothing_2008.csv"
    output_path = "hdfs://namenode:9000/alp/output/batch_analysis"

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(input_path)
    )

    clickstream_df = (
        df
        .withColumnRenamed("session ID", "session_id")
        .withColumnRenamed("page 1 (main category)", "main_category")
        .withColumnRenamed("page 2 (clothing model)", "clothing_model")
        .withColumnRenamed("price 2", "price_level")
        .withColumnRenamed("order", "order_in_session")
    )

    summary_df = clickstream_df.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("unique_sessions"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.min("price").alias("minimum_price"),
        F.max("price").alias("maximum_price")
    )

    top_categories_df = (
        clickstream_df
        .groupBy("main_category")
        .count()
        .orderBy(F.desc("count"))
    )

    top_models_df = (
        clickstream_df
        .groupBy("clothing_model")
        .count()
        .orderBy(F.desc("count"))
    )

    country_distribution_df = (
        clickstream_df
        .groupBy("country")
        .count()
        .orderBy(F.desc("count"))
    )

    avg_price_by_category_df = (
        clickstream_df
        .groupBy("main_category")
        .agg(
            F.round(F.avg("price"), 2).alias("average_price"),
            F.count("*").alias("total_views")
        )
        .orderBy("main_category")
    )

    page_distribution_df = (
        clickstream_df
        .groupBy("page")
        .count()
        .orderBy("page")
    )

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

    summary_df.write.mode("overwrite").json(f"{output_path}/summary")
    top_categories_df.write.mode("overwrite").json(f"{output_path}/top_categories")
    top_models_df.write.mode("overwrite").json(f"{output_path}/top_models")
    country_distribution_df.write.mode("overwrite").json(f"{output_path}/country_distribution")
    avg_price_by_category_df.write.mode("overwrite").json(f"{output_path}/avg_price_by_category")
    page_distribution_df.write.mode("overwrite").json(f"{output_path}/page_distribution")

    print(f"\nBatch analysis output saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()