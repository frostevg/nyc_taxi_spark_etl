from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark(app_name: str = "NYCTaxiETL") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def run_etl():
    spark = create_spark()

    # Use your real parquet file here:
    input_path = "data/raw/yellow_tripdata_2025-01.parquet"
    output_path = "data/processed/trips_cleaned"

    print(f"Reading input from: {input_path}")
    df = spark.read.parquet(input_path)

    print("Input schema:")
    df.printSchema()

    # Example cleaning step: filter out rows with null pickup/dropoff
    df_clean = df.where(
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull())
    )

    print("Writing cleaned data to:", output_path)
    (
        df_clean
        .coalesce(1)  # fewer output files for local testing
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    run_etl()


