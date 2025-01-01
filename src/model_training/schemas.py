from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DateType, DoubleType, ArrayType, LongType

# Category Dataset Schema
category_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("cat_level1", StringType(), True),
    StructField("cat_level2", StringType(), True),
    StructField("cat_level3", StringType(), True)
])

# Customer Dataset Schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthdate", DateType(), True),
    StructField("first_join_date", StringType(), True),
])

# Product Dataset Schema
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("baseColour", StringType(), True),
    StructField("season", StringType(), True),
    StructField("usage", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("productDisplayName", StringType(), True),
    StructField("category_id", IntegerType(), True)
])

# Browsing Behavior Dataset Schema
browsing_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("device_type", StringType(), True),
])

# Transaction Dataset Schema
transaction_schema = StructType([
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
])