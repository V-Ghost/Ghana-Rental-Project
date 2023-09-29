# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, desc, asc, avg, max, lower, min
import os
import glob
import boto3
import fnmatch
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder \
    .appName("") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .getOrCreate()

# Read data from S3 into a DataFrame
df = spark.read.parquet("", header=True, inferSchema=True)

# Cache the DataFrame for improved performance
df.cache()

# Remove digits from the "interior" column
df = df.withColumn("interior", regexp_replace(col("interior"), "\\d+", ""))


# Remove non-numeric characters from "bedrooms" and cast it to an integer, then order it in ascending order
df = df.withColumn("bedrooms", regexp_replace(col("bedrooms"), "[a-zA-Z]", "").cast("integer")).orderBy(asc("bedrooms"))

# Remove non-numeric characters from "washrooms" and cast it to an integer, then order it in ascending order
df = df.withColumn("washrooms", regexp_replace(col("washrooms"), "[a-zA-Z]", "").cast("integer")).orderBy(asc("washrooms"))

# Cast "price" to an integer
df = df.withColumn("price", col("price").cast("integer"))

# Remove non-numeric characters from "area" and cast it to an integer, then order it in ascending order
df = df.withColumn("area", regexp_replace(col("area"), "[a-zA-Z]", "").cast("integer")).orderBy(asc("area"))

# Convert "landlord," "description," "city," "suburb," and "interior" columns to lowercase
df = df.withColumn("landlord", lower(col("landlord")))
df = df.withColumn("description", lower(col("description")))
df = df.withColumn("city", lower(col("city")))
df = df.withColumn("suburb", lower(col("suburb")))
df = df.withColumn("interior", lower(col("interior")))

# Write the transformed DataFrame back to S3
df.write.mode("overwrite").parquet("")

# Stop the Spark session
spark.stop()
