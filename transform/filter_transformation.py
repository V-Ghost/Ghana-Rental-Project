# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, split
import os
import glob
import boto3
import fnmatch
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder \
    .appName("S3 transformations") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .getOrCreate()

# Read CSV from the S3 data lake (Replace "" with your S3 path)
result = spark.read.csv("")  # Replace "" with your S3 path

# Cache the DataFrame for improved performance
result.cache()

# Remove commas from the "price" column
result = result.withColumn("price", regexp_replace(col("price"), ",", ""))

# Remove "GH₵" from the "price" column
result = result.withColumn("price", regexp_replace(col("price"), "GH₵", ""))

# Cast the "price" column to an integer
result = result.withColumn("price", col("price").cast("integer"))

# Create a new DataFrame "null_filtered" with rows where specific columns are not null
null_filtered = result.filter(
    (col("price").isNotNull()) &
    (col("location").isNotNull()) &
    (col("description").isNotNull()) &
    (col("tags").isNotNull()) &
    (col("landlord").isNotNull()) &
    (col("img_links").isNotNull()) &
    (col("contacts").isNotNull())
)

# Split the "location" column into "city" and "suburb" columns
null_filtered = null_filtered.withColumn("city", split(null_filtered["location"], ',')[0])
null_filtered = null_filtered.withColumn("suburb", split(null_filtered["location"], ',')[1])

# Split the "suburb" column further if needed
null_filtered = null_filtered.withColumn("suburb", split(null_filtered["suburb"], "/")[0])

# Select and show the "city" and "suburb" columns
null_filtered.select("city", "suburb").show()

# Drop the "location" column
null_filtered = null_filtered.drop("location")

# Extract and format information from the "tags" column into new columns
null_filtered = null_filtered.withColumn("bedrooms", split(null_filtered["tags"], "                      ")[2].cast("integer"))
null_filtered = null_filtered.withColumn("washrooms", split(null_filtered["tags"], "                      ")[3])
null_filtered = null_filtered.withColumn("interior", split(null_filtered["tags"], "                      ")[1])
null_filtered = null_filtered.withColumn("area", split(null_filtered["tags"], "                      ")[0])
null_filtered = null_filtered.withColumn("area", regexp_replace(col("area"), "sqm", ""))

# Select and show relevant columns
null_filtered.select("bedrooms", "washrooms", "interior", "area")

# Drop the "tags" column
null_filtered = null_filtered.drop("tags")


# Write the transformed DataFrame back to S3
null_filtered.write.mode("overwrite").parquet("")
