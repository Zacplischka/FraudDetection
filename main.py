# Orchestrate the whole application
from src.model_training import preprocessing
from src.model_training import model
from src.model_training import spark
import os
import subprocess
# Define paths and schemas
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DateType, DoubleType, ArrayType, LongType

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

jdbc_driver_path = "database/postgresql-42.7.4.jar"

spark = spark.create_spark_session(
    app_name="FraudDetection",
    max_partition_bytes="12000000",
    master="local[*]",
    executor_memory="16g",
    driver_memory="16g",
    adaptive_enabled=True,
    log_level="ERROR",
    jdbc_driver_path=jdbc_driver_path
)

# Process data
data = preprocessing.preprocess_data(data_paths=data_paths, schemas=schemas, spark=spark)

# retrain model (optional)
model_path = "src/model_training/gb_model"  # or wherever your model should be saved

if not os.path.exists(model_path):
    print(f"Model not found at {model_path}. Training a new model...")
    subprocess.run(["python", "scripts/train_model.py"])
else:
    print("Model already exists. Skipping training...")

#load model
from pyspark.ml import PipelineModel
# Load the model
model = PipelineModel.load(model_path)

# Use the model for predictions
print("Making predictions...")
predictions = model.transform(data)
print("Predictions completed.")

# write data to data/output as a parquet file
print("Saving predictions...")
output_path = "data/output/predictions"
predictions.write.mode("overwrite").parquet(output_path)

predictions_for_db = predictions.select("transaction_id", "prediction")

db_properties = {
    "user": "docker",
    "password": "docker",
    "driver": "org.postgresql.Driver"
}
jdbc_url = "jdbc:postgresql://localhost:5432/exampledb"

# Write the predictions to the database
predictions_for_db.write.jdbc(url=jdbc_url, table="predictions", mode="overwrite", properties=db_properties)

print(f"Predictions saved to {output_path}")

