
# Imports libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace,col
import os
import glob
import boto3
import fnmatch
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row


# Create sparksession
spark = SparkSession.builder \
    .appName("S3 DataLake for ETL") \
    .getOrCreate()

# Read csv from the s3 data lake
result = spark.read.csv("")  # Replace "" with your S3 path

# Function to check if a defined column is present in the DataFrame
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

# Define S3 path prefix (Replace "" with your S3 prefix)
prefix = ''

# Initialize the Boto3 S3 client
s3 = boto3.client('s3')

# Define the S3 bucket name (Replace "" with your S3 bucket name)
bucket_name = ''

# List objects in the S3 bucket with the given prefix
s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Define CSV file extension pattern
pattern = '*.csv'

# Find all links in the defined S3 URI that match the CSV file extension pattern
# and store them in an array assigned to the variable name "matching_objects"
matching_objects = [obj['Key'] for obj in s3_objects.get('Contents', []) if fnmatch.fnmatch(obj['Key'], pattern)]

# Read the first CSV in the S3 URI by accessing the 0th element in "matching_objects"
# and convert it into a DataFrame assigned to the variable name "df"
df = spark.read.csv(f's3a://{bucket_name}/{matching_objects[0]}', header=True, inferSchema=True)

# Modify the "matching_objects" array by removing the first element
# This is done so that the first element can be used as the initial DataFrame for the union transformations
matching_objects = matching_objects[1:]

# Transform the first DataFrame "df" into the desired format for the upcoming union transformations
if has_column(df, "webpages") is False:
    df = df.withColumn("webpages", lit("Not Available"))
if has_column(df, "period") is True:
    df = df.drop("period")

# Iterate through all elements in the array "matching_objects"
for key in matching_objects:
    # On each iteration, combine the element with the predefined bucket name
    # and read it as a CSV into a DataFrame assigned to the variable name "result"
    result = spark.read.csv(f's3a://{bucket_name}/{key}', header=True, inferSchema=True)
    
    # Perform transformations based on the structure of the CSV
    # Check if the column "webpages" exists in the DataFrame "result"
    if has_column(result, "webpages") is False:
        # If the column doesn't exist, add a new column "webpages" with the value "Not Available"
        result = result.withColumn("webpages", lit("Not Available"))

    # Check if the column "period" exists in the DataFrame "result"
    if has_column(result, "period") is True:
        # If the column exists, drop (remove) the "period" column from the DataFrame
        result = result.drop("period")

    # Check if the column "contacts" exists in the DataFrame "result"
    if has_column(result, "contacts") is False:
        # If the column doesn't exist, add a new column "contacts" with the value "Not Available"
        result = result.withColumn("contacts", lit("Not Available"))

    
    # Perform a union transformation between the DataFrame objects "df" and "result"
    # and reassign the resulting DataFrame to the variable name "df"
    df = df.union(result)

# Write the DataFrame "df" into the S3 data lake in Parquet format (Replace "" with your S3 path)
df.write.parquet("")  # Replace "" with your S3 path

# Destroy the current Spark session
spark.stop()