import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, hour, dayofweek, month

RAW_DATA_PATH = "../data/raw/yellow_tripdata_*.parquet"
PROCESSED_DATA_PATH = "../data/processed/cleaned_yellow_taxi.parquet"

def create_spark_session():
    """
    Create a Spark session with the necessary configurations.
    """
    spark = SparkSession.builder \
        .appName("NYC Taxi Data Preprocessing") \
        .master("local[*]") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark


def load_data(spark):
    """
    Load the raw NYC taxi data from Parquet files.
    """
    df = spark.read.parquet(RAW_DATA_PATH)

    columns_subset = [
        'tpep_pickup_datetime', 
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'PULocationID',
        'DOLocationID',
        'fare_amount',
        'total_amount'
    ]

    return df.select(*columns_subset)


def parse_dates(df):
    """
    Parse date columns to timestamp format.
    """
    df = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
           .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    return df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")


def filter_invalid_data(df):
    """Filtra righe non valide (valori nulli o negativi)"""
    return df.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0) &
        (col("passenger_count").between(0, 6)) &
        ((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) > 0)
    )


def compute_new_features(df):
    """Calcola nuove feature utili per l'analisi"""
    df = df.withColumn("trip_duration_min", 
                       (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)

    df = df.filter(col("trip_duration_min") < 180)  # Massimo 3 ore

    df = df.withColumn("avg_speed_mph", 
                       col("trip_distance") / (col("trip_duration_min") / 60))

    df = df.withColumn("pickup_hour", hour("pickup_datetime")) \
           .withColumn("pickup_day_of_week", dayofweek("pickup_datetime")) \
           .withColumn("pickup_month", month("pickup_datetime"))
    
    return df

def rename_and_reorder_columns(df):
    """Rinomina e riordina le colonne per chiarezza"""
    df = df.withColumnRenamed("PULocationID", "pickup_zone") \
           .withColumnRenamed("DOLocationID", "dropoff_zone")

    return df.select(
        "pickup_datetime", "dropoff_datetime",
        "pickup_hour", "pickup_day_of_week", "pickup_month",
        "passenger_count", "trip_distance", "trip_duration_min", "avg_speed_mph",
        "fare_amount", "total_amount",
        "pickup_zone", "dropoff_zone"
    )

def save_processed_data(df):
    """Salva i dati preprocessati in formato parquet"""
    output_path = PROCESSED_DATA_PATH
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Dati preprocessati salvati in: {output_path}")

def main():
    """
    Main function to run the preprocessing steps.
    """

    print("🚀 Avvio del processo di preprocessing...")
    spark = create_spark_session()
    
    print("📥 Caricamento e selezione colonne...")
    df = load_data(spark)

    print("📅 Parsing delle date...")
    df = parse_dates(df)

    print("🧹 Filtro dati non validi...")
    df = filter_invalid_data(df)

    print("🧮 Calcolo nuove feature...")
    df = compute_new_features(df)

    print("🔄 Rinomina e riordino colonne...")
    df = rename_and_reorder_columns(df)

    print("💾 Salvataggio dati preprocessati...")
    save_processed_data(df)

    print("🎉 Preprocessing completato!")

    spark.stop()


if __name__ == "__main__":
    main()    