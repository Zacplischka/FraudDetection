# Orchestrate the whole application
from src.model_training import preprocessing
from src.model_training import model
import os
import subprocess

# Define paths and schemas
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType,
    TimestampType, DateType, DoubleType
)

from src.model_training.preprocessing import load_data

# Erase predictions folder
subprocess.run(["rm", "-rf", "data/output/predictions"])

# Define schemas for each dataset
schemas = {
    # Category Dataset Schema
    'category': StructType([
        StructField("category_id", IntegerType(), True),
        StructField("cat_level1", StringType(), True),
        StructField("cat_level2", StringType(), True),
        StructField("cat_level3", StringType(), True)
    ]),
    # Customer Dataset Schema
    'customer': StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthdate", DateType(), True),
        StructField("first_join_date", StringType(), True)
    ]),
    # Product Dataset Schema
    'product': StructType([
        StructField("product_id", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("baseColour", StringType(), True),
        StructField("season", StringType(), True),
        StructField("usage", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("productDisplayName", StringType(), True),
        StructField("category_id", IntegerType(), True)
    ]),
    # Browsing Behavior Dataset Schema
    'browsing_behaviour': StructType([
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("device_type", StringType(), True)
    ]),
    # Transaction Dataset Schema
    'transactions': StructType([
        StructField("created_at", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_metadata", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("promo_amount", FloatType(), True),
        StructField("promo_code", StringType(), True),
        StructField("shipment_fee", FloatType(), True),
        StructField("shipment_location_lat", DoubleType(), True),
        StructField("shipment_location_long", DoubleType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("clear_payment", IntegerType(), True)
    ]),
    'fraud_transaction': StructType([
        StructField("transaction_id", StringType(), True),
        StructField("is_fraud", StringType(), True)
    ])
}

data_paths = {
    'category': 'data/input/category.csv',
    'customer': 'data/input/customer.csv',
    'fraud_transaction': 'data/input/fraud_transaction.csv',
    'transactions': 'data/input/new_transactions.csv',
    'browsing_behaviour': 'data/input/new_browsing_behaviour.csv',
    'product': 'data/input/product.csv'
}

# --------------------------------------------------------------------------
# PySpark + PostgreSQL config

from pyspark.sql import SparkSession
from pyspark import SparkConf

# Path to the JDBC driver **inside the Docker container**:
jdbc_driver_path = ".venv/lib/python3.11/site-packages/pyspark/jars/postgresql-42.7.4.jar"

# PostgreSQL connection info:
postgres_host = "database"  # Docker service name
postgres_port = "5432"
postgres_db = "fraud"
postgres_user = os.getenv("DB_USER", "docker")    # e.g., from env variable
postgres_password = os.getenv("DB_PASSWORD", "docker")

# Define JDBC connection properties
db_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}
jdbc_url = "jdbc:postgresql://localhost:5432/fraud"

# SparkConf with JAR in the classpath
conf = (SparkConf()
    .setAppName("MySparkApp")
    .setMaster("local[*]")
    .set("spark.sql.files.maxPartitionBytes", "12000000")
    .set("spark.executor.memory", "16g")
    .set("spark.driver.memory", "16g")
    .set("spark.sql.shuffle.partitions", "10")
    # Make sure Spark sees the driver:
    .set("spark.jars", jdbc_driver_path)
    .set("spark.driver.extraClassPath", jdbc_driver_path)
    .set("spark.executor.extraClassPath", jdbc_driver_path)
)

# Initialize Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Clear Spark catalog cache
spark.catalog.clearCache()

print("✅ Spark session created successfully.")

# --------------------------------------------------------------------------
# Process data
data = preprocessing.preprocess_data(data_paths=data_paths, schemas=schemas, spark=spark)

# --------------------------------------------------------------------------
# Model training or loading
model_path = "src/model_training/gb_model"

if not os.path.exists(model_path):
    print(f"Model not found at {model_path}. Training a new model...")
    subprocess.run(["python", "src/model_training/train_model.py"])
else:
    print("Model already exists. Skipping training...")

# Load the trained model
from pyspark.ml import PipelineModel
model = PipelineModel.load(model_path)

# --------------------------------------------------------------------------
# Predictions
print("Making predictions...")
predictions = model.transform(data)
print("Predictions completed.")

print('Generating city features for visualization...')
# Convert to Pandas and add city features
predictions_df = predictions.toPandas()
df_with_city = preprocessing.add_city_features(predictions_df, "shipment_location_lat", "shipment_location_long")
withCity = spark.createDataFrame(df_with_city)

# --------------------------------------------------------------------------
# Save predictions to parquet
print("Saving predictions...")
output_path = "data/output/predictions"
predictions.write.mode("overwrite").parquet(output_path)

print(f"Predictions saved to {output_path}")

# --------------------------------------------------------------------------
# Write predictions to database
predictions_for_db = predictions.select(
    "transaction_id", "payment_method", "total_amount", "L1_ratio",
    "clear_payment", "purchases_count", "is_fraud", "prediction"
)

withCity = withCity.select(
    "transaction_id", "payment_method", "total_amount", "L1_ratio",
    "clear_payment", "purchases_count", "shipment_location_lat",
    "shipment_location_long", "city", "is_fraud", "prediction"
)

# Overwrite table = drop & recreate "predictions" inside Postgres
withCity.write.jdbc(
    url=jdbc_url,
    table="predictions",
    mode="overwrite",
    properties=db_properties
)

print("✅ Predictions successfully written to the database.")
