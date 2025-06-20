from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIOApp") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .getOrCreate()

# Read from MinIO
try:
    df = spark.read.parquet("s3a://nyc/yellow_tripdata_2025-03.parquet")
    print("Successfully loaded data")
    print(f"Total records: {df.count()}")
    print("Schema:")
    df.printSchema()
except Exception as e:
    print(f"Error loading data: {str(e)}")

finally:
    spark.stop()
