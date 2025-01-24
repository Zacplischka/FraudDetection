
from src.model_training import preprocessing
from src.model_training import model
from src.model_training import spark


# Define paths and schemas
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, DoubleType

spark = spark.create_spark_session()


# Data paths dictionary
data_paths = {
    'category': 'data/input/category.csv',
    'customer': 'data/input/customer.csv',
    'fraud_transaction': 'data/input/fraud_transaction.csv',
    'transactions': 'data/input/transactions.csv',
    'browsing_behaviour': 'data/input/browsing_behaviour.csv',
    'product': 'data/input/product.csv'
}

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


# Define categorical and numeric columns
categorical_columns = ['payment_method', 'age_bin']
numeric_columns = ['account_age', 'promo_code_used', 'total_amount', 'purchases_count', 'L1_ratio', 'total_browsing_time']

if __name__ == "__main__":
    # Preprocess data, train models, and save the best model
    rf_model, gbt_model = model.preprocess_and_train(
        data_paths=data_paths,
        schemas=schemas,
        spark=spark,
        categorical_columns=categorical_columns,
        numeric_columns=numeric_columns,
        model_save_path="src/model_training/gb_model"
    )
    print("Model training complete.")
    spark.stop()
