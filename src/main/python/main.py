"""
Main ETL pipeline for enriching hotel data with weather information.

Pipeline steps:
    1. Load hotel data from S3 and encrypt PII fields (Name, Address).
    2. Fill missing coordinates via OpenCage Geocoding API.
    3. Compute geohash for each hotel based on its coordinates.
    4. Load weather data from S3, filtered to the bounding box of hotel locations.
    5. Compute geohash for each weather record.
    6. Left join hotels with weather on geohash.
    7. Write the enriched result back to S3 partitioned by year/month/day.

Environment variables required:
    BUCKET_NAME: Name of the AWS S3 bucket containing input and output data.
    OPENCAGE_API_KEY: API key for the OpenCage Geocoding service.
    AES_KEY: 16/24/32-character key used for AES encryption of PII fields.
"""

import os
import sys
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, min, max, pandas_udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv

from encryption import CryptoUtils
from geo import GeoUtils

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("BUCKET_NAME")


@pandas_udf(StringType())
def geohash_pandas_udf(lat: pd.Series, lng: pd.Series) -> pd.Series:
    """Pandas UDF that encodes lat/lng coordinate pairs into geohash strings.

    Uses Apache Arrow for batched processing, which is significantly more
    memory-efficient than a row-by-row UDF for large datasets like weather data.

    Args:
        lat (pd.Series): Series of latitude values.
        lng (pd.Series): Series of longitude values.

    Returns:
        pd.Series: Series of geohash strings with precision=4 (~40km grid).

    Example:
        >>> weather_with_gh = weather_df.withColumn(
        ...     "geohash", geohash_pandas_udf(col("lat"), col("lng"))
        ... )
    """
    import pygeohash as gh
    return pd.Series([gh.encode(l, n, precision=4) for l, n in zip(lat, lng)])


if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .appName("HotelWeatherETL") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    # Distribute custom modules to all Spark workers so UDFs can import them
    spark.sparkContext.addPyFile("/opt/src/main/python/encryption.py")
    spark.sparkContext.addPyFile("/opt/src/main/python/geo.py")

    # UDF for geocoding city/country pairs via OpenCage API (used for missing coordinates)
    geocode_udf = udf(GeoUtils.get_coords_from_api, GeoUtils.get_schema())

    # Row-by-row UDF for hotels — dataset is small so performance is acceptable
    geohash_simple_udf = udf(
        lambda lt, lg: __import__('pygeohash').encode(float(lt), float(lg), precision=4)
        if lt is not None and lg is not None else None,
        StringType()
    )

    logger.info("ETL job started. Loading hotel data from s3a://%s/data/hotels", BUCKET_NAME)

    # Load raw hotel CSV and encrypt PII columns
    hotels_raw = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"s3a://{BUCKET_NAME}/data/hotels")
    hotels_encrypted = CryptoUtils.encrypt_pii_data(hotels_raw, ["Name", "Address"])

    # Find hotels with missing coordinates and enrich them via geocoding API
    missing = hotels_encrypted.filter(col("Latitude").isNull() | col("Longitude").isNull()) \
                              .select("City", "Country").distinct()

    enriched_map = missing \
        .withColumn("api_res", geocode_udf(col("City"), col("Country"))) \
        .select("City", "Country",
                col("api_res.lat").alias("new_lat"),
                col("api_res.lng").alias("new_lng")) \
        .cache()

    # Fill missing coordinates with API results where available
    hotels_final = hotels_encrypted.join(enriched_map, ["City", "Country"], "left") \
        .withColumn("Latitude", when(col("Latitude").isNull(), col("new_lat"))
                    .otherwise(col("Latitude").cast("double"))) \
        .withColumn("Longitude", when(col("Longitude").isNull(), col("new_lng"))
                    .otherwise(col("Longitude").cast("double"))) \
        .drop("new_lat", "new_lng")

    # Compute geohash for hotels that have valid coordinates
    hotels_with_gh = hotels_final \
        .filter(col("Latitude").isNotNull() & col("Longitude").isNotNull()) \
        .withColumn("geohash", geohash_simple_udf(col("Latitude"), col("Longitude"))) \
        .cache()

    # Compute bounding box of hotel coordinates to filter weather data
    b = hotels_with_gh.select(
        min("Latitude"), max("Latitude"),
        min("Longitude"), max("Longitude")
    ).collect()[0]

    logger.info(
        "Hotels processed. Bounding box: lat=[%.4f, %.4f], lng=[%.4f, %.4f]",
        b[0], b[1], b[2], b[3]
    )

    # Load weather data filtered to hotel bounding box (+0.1 degree buffer)
    # This avoids loading global weather data when hotels cover only a specific region
    weather_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/data/weather") \
        .filter(
            col("lat").between(b[0] - 0.1, b[1] + 0.1) &
            col("lng").between(b[2] - 0.1, b[3] + 0.1)
        ) \
        .repartition(32)  # Distribute evenly across workers before pandas_udf

    # Compute geohash for weather records using pandas_udf for memory efficiency
    weather_with_gh = weather_df.withColumn(
        "geohash", geohash_pandas_udf(col("lat"), col("lng"))
    )

    # Left join ensures all hotels are retained even if no weather data matches
    final_result = hotels_with_gh.join(weather_with_gh, "geohash", "left")

    logger.info("Starting write to s3a://%s/data/final_enriched_output", BUCKET_NAME)

    # Write partitioned by date for efficient time-based querying
    final_result.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(f"s3a://{BUCKET_NAME}/data/final_enriched_output")

    logger.info("Write complete. Showing sample results.")

    final_result.select("City", "Name", "geohash", "avg_tmpr_c", "wthr_date") \
        .show(20, truncate=False)

    logger.info("ETL job finished successfully.")