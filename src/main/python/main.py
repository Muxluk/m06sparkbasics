import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, min, max, pandas_udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv

from encryption import CryptoUtils
from geo import GeoUtils

load_dotenv()

BUCKET_NAME = "m06awsbucket-p7yolbkc"

@pandas_udf(StringType())
def geohash_pandas_udf(lat: pd.Series, lng: pd.Series) -> pd.Series:
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

    spark.sparkContext.addPyFile("/opt/src/main/python/encryption.py")
    spark.sparkContext.addPyFile("/opt/src/main/python/geo.py")
    
    geocode_udf = udf(GeoUtils.get_coords_from_api, GeoUtils.get_schema())
    
    geohash_simple_udf = udf(lambda lt, lg: __import__('pygeohash').encode(float(lt), float(lg), precision=4) 
                             if lt is not None and lg is not None else None, StringType())

    print("--- Hotels & AES Encryption ---")
    hotels_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(f"s3a://{BUCKET_NAME}/data/hotels")
    hotels_encrypted = CryptoUtils.encrypt_pii_data(hotels_raw, ["Name", "Address"])

    missing = hotels_encrypted.filter(col("Latitude").isNull() | col("Longitude").isNull()) \
                              .select("City", "Country").distinct()
    
    enriched_map = missing.withColumn("api_res", geocode_udf(col("City"), col("Country"))) \
                          .select("City", "Country", col("api_res.lat").alias("new_lat"), col("api_res.lng").alias("new_lng")).cache()

    hotels_final = hotels_encrypted.join(enriched_map, ["City", "Country"], "left") \
        .withColumn("Latitude", when(col("Latitude").isNull(), col("new_lat")).otherwise(col("Latitude").cast("double"))) \
        .withColumn("Longitude", when(col("Longitude").isNull(), col("new_lng")).otherwise(col("Longitude").cast("double"))) \
        .drop("new_lat", "new_lng")

    hotels_with_gh = hotels_final.filter(col("Latitude").isNotNull() & col("Longitude").isNotNull()) \
                                 .withColumn("geohash", geohash_simple_udf(col("Latitude"), col("Longitude"))).cache()

    b = hotels_with_gh.select(min("Latitude"), max("Latitude"), min("Longitude"), max("Longitude")).collect()[0]

    print("--- Weather Processing ---")
    weather_df = spark.read.parquet(f"s3a://{BUCKET_NAME}/data/weather") \
        .filter(col("lat").between(b[0]-0.1, b[1]+0.1) & col("lng").between(b[2]-0.1, b[3]+0.1)) \
        .repartition(32) 

    weather_with_gh = weather_df.withColumn("geohash", geohash_pandas_udf(col("lat"), col("lng")))

    print("--- Final Left Join ---")
    final_result = hotels_with_gh.join(weather_with_gh, "geohash", "left")

    print("--- Storing Results ---")
    final_result.write.mode("overwrite").partitionBy("year", "month", "day").parquet(f"s3a://{BUCKET_NAME}/data/final_enriched_output")
    
    final_result.select("City", "Name", "geohash", "avg_tmpr_c", "wthr_date").show(20, truncate=False)
    print("ETL Job Finished.")