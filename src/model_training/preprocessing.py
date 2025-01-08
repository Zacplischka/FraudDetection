from typing import Dict

import pandas
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, ArrayType
)
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, min, max, expr, hour, when, from_json, explode, sum as spark_sum
)
import src.model_training.spark



def load_data(data_paths: Dict[str, str],
              schemas: Dict[str, StructType],
              spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Load multiple datasets from CSV files into Spark DataFrames.

    :param data_paths: A dictionary where keys are dataset names and values are file paths.
    :param schemas: A dictionary where keys are dataset names and values are the schemas for those datasets.
    :param spark: An active SparkSession instance.
    :return: A dictionary containing dataset names as keys and their corresponding Spark DataFrames as values.
    """
    dataframes = {}
    try:
        for dataset_name, file_path in data_paths.items():
            schema = schemas.get(dataset_name)
            if schema:
                dataframes[dataset_name] = spark.read.csv(file_path, header=True, schema=schema)
                print(f"Loaded dataset '{dataset_name}' from '{file_path}'.")
            else:
                print(f"Schema not provided for '{dataset_name}'. Skipping...")
        print("All data loaded successfully.")
    except Exception as error:
        print("An error occurred while loading the data:")
        print(str(error))

    return dataframes


def add_event_level_features(transactions_df: DataFrame,
                             browsing_behaviour_df: DataFrame) -> DataFrame:
    """
    Add event level counts (L1, L2, L3) as features to the transactions DataFrame.

    :param transactions_df: Spark DataFrame containing transaction data.
    :param browsing_behaviour_df: Spark DataFrame containing browsing behavior data.
    :return: A Spark DataFrame with added event level counts (L1_count, L2_count, L3_count) per transaction.
    """
    # Define the expression to map event types to event levels
    event_level_expr = (
        F.when(F.col('event_type').isin('AP', 'ATC', 'CO'), 'L1')
         .when(F.col('event_type').isin('VC', 'VP', 'VI', 'SER'), 'L2')
         .otherwise('L3')
    )

    # Add event_level column to browsing_behaviour_df
    browsing_behaviour_df = browsing_behaviour_df.withColumn('event_level', event_level_expr)

    # Join transactions_df with browsing_behaviour_df on 'session_id'
    joined_df = transactions_df.join(
        browsing_behaviour_df,
        on='session_id',
        how='inner'
    )

    # Group by transaction_id and pivot event levels to count occurrences
    pivoted_df = (
        joined_df.groupBy('transaction_id')
        .pivot('event_level')
        .count()
        .fillna(0)
    )

    # Cast counts to IntegerType and rename columns to include '_count'
    event_level_columns = [c for c in pivoted_df.columns if c != 'transaction_id']
    for col_name in event_level_columns:
        pivoted_df = pivoted_df.withColumn(
            f"{col_name}_count",
            F.col(col_name).cast(IntegerType())
        ).drop(col_name)

    # Join the pivoted event level counts back to transactions_df
    feature_df = transactions_df.join(pivoted_df, on='transaction_id', how='left')

    return feature_df


def add_event_level_ratios(feature_df: DataFrame) -> DataFrame:
    """
    Add L1_ratio and L2_ratio columns to the feature DataFrame.
    Ratios are calculated as percentages of the respective event level counts
    relative to the total count of all event levels per transaction.

    :param feature_df: Spark DataFrame containing transaction data with event level counts.
    :return: Spark DataFrame with added L1_ratio and L2_ratio columns.
    """
    feature_df = feature_df.withColumn(
        "L1_ratio",
        F.round(
            (F.col("L1_count").cast(FloatType()) /
             (F.col("L1_count") + F.col("L2_count") + F.col("L3_count")).cast(FloatType())) * 100,
            2
        ).cast(FloatType())
    )

    feature_df = feature_df.withColumn(
        "L2_ratio",
        F.round(
            (F.col("L2_count").cast(FloatType()) /
             (F.col("L1_count") + F.col("L2_count") + F.col("L3_count")).cast(FloatType())) * 100,
            2
        ).cast(FloatType())
    )

    return feature_df


def calculate_session_times(browsing_behaviour_df: DataFrame) -> DataFrame:
    """
    Computes start time, finish time, midpoint time, hour, and time of day for each session.

    :param browsing_behaviour_df: Spark DataFrame containing browsing behavior data.
                                  Expected to have 'session_id' and 'event_time' columns.
    :return: A Spark DataFrame with session times and time-of-day classification.
    """
    time_df = browsing_behaviour_df.groupBy("session_id").agg(
        min("event_time").alias("start_time"),
        max("event_time").alias("finish_time")
    )

    time_df = time_df.withColumn(
        "midpoint_time",
        expr("start_time + (finish_time - start_time) / 2")
    )

    browsing_df = time_df.withColumn(
        "hour", hour("midpoint_time")
    ).withColumn(
        "time_of_day",
        when((col("hour") >= 0) & (col("hour") < 6), "night")
        .when((col("hour") >= 6) & (col("hour") < 12), "morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
        .otherwise("evening")
    )

    return browsing_df


def enrich_feature_data(
    feature_df: DataFrame,
    browsing_df: DataFrame,
    customer_df: DataFrame
) -> DataFrame:
    """
    Enrich the feature DataFrame by calculating customer age and join year, adding geolocation,
    and joining with browsing and customer information.

    :param feature_df: Spark DataFrame containing the main features.
    :param browsing_df: Spark DataFrame containing session and browsing data.
    :param customer_df: Spark DataFrame containing customer data.
                        Must include 'birthdate' and 'first_join_date' columns.
    :return: Enriched Spark DataFrame with additional customer and session information.
    """
    customer_df = customer_df.withColumn(
        "age", F.floor(F.datediff(F.current_date(), F.col("birthdate")) / 365)
    ).withColumn(
        "first_join_year", F.year("first_join_date")
    ).select("customer_id", "age", "first_join_year", "gender", "first_join_date")

    enriched_feature_df = feature_df.withColumn(
        "geolocation",
        F.concat(F.col("shipment_location_lat"), F.lit(","), F.col("shipment_location_long"))
    ).join(
        browsing_df, on=["session_id"], how="inner"
    ).join(
        customer_df, on=["customer_id"], how="inner"
    ).dropDuplicates(["transaction_id"])

    return enriched_feature_df


def add_purchases_count(feature_df: DataFrame,
                        transactions_df: DataFrame) -> DataFrame:
    """
    Add the total purchases count per transaction to the feature DataFrame by parsing
    and aggregating the product_metadata column.

    :param feature_df: Spark DataFrame containing the main features.
    :param transactions_df: Spark DataFrame containing transaction data with a product_metadata column.
    :return: Spark DataFrame enriched with purchases count for each transaction.
    """
    product_metadata_schema = ArrayType(StructType([
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("item_price", IntegerType(), True)
    ]))

    transactions_parsed_df = transactions_df.withColumn(
        "product_metadata_parsed",
        from_json(col("product_metadata"), product_metadata_schema)
    )

    df_exploded = transactions_parsed_df.withColumn(
        "product_metadata_exploded",
        explode("product_metadata_parsed")
    )

    df_features = df_exploded.select(
        col("transaction_id"),
        col("product_metadata_exploded.product_id").alias("product_id"),
        col("product_metadata_exploded.quantity").alias("quantity"),
        col("product_metadata_exploded.item_price").alias("item_price")
    ).groupBy("transaction_id").agg(
        spark_sum("quantity").alias("purchases_count")
    )

    enriched_feature_df = feature_df.join(df_features, on="transaction_id", how="inner")
    return enriched_feature_df


def add_fraud_labels(feature_df: DataFrame,
                     fraud_trans_file: str,
                     spark: SparkSession) -> DataFrame:
    """
    Add fraud labels to the feature DataFrame by joining with fraud transaction data.

    :param feature_df: Spark DataFrame containing the main features.
    :param fraud_trans_file: Path to the fraud transaction CSV file.
    :param spark: SparkSession instance to read the fraud transaction data.
    :return: Spark DataFrame enriched with fraud labels and cached for faster processing.
    """
    fraud_trans_df = spark.read.csv(fraud_trans_file, header=True)

    feature_df = (feature_df
                  .join(fraud_trans_df, on=["transaction_id"], how="left")
                  .withColumn("is_fraud", when(col("is_fraud") == "true", 1).otherwise(0)))

    feature_df.cache()

    return feature_df


def add_account_and_promo_features(feature_df: DataFrame) -> DataFrame:
    """
    Add account age, age bins, and promo code usage features to the DataFrame.

    :param feature_df: Spark DataFrame containing customer and transaction data.
                       Expected to include 'first_join_date', 'age', and 'promo_code' columns.
    :return: DataFrame with additional columns: 'account_age', 'age_bin', and 'promo_code_used'.
    """
    enriched_df = feature_df.withColumn(
        "account_age",
        F.floor(F.datediff(F.current_date(), F.col("first_join_date")) / 365)
    ).withColumn(
        "age_bin",
        F.when(F.col("age") < 10, "<10")
         .when((F.col("age") >= 10) & (F.col("age") < 20), "10-19")
         .when((F.col("age") >= 20) & (F.col("age") < 30), "20-29")
         .when((F.col("age") >= 30) & (F.col("age") < 40), "30-39")
         .when((F.col("age") >= 40) & (F.col("age") < 50), "40-49")
         .when((F.col("age") >= 50) & (F.col("age") < 60), "50-59")
         .otherwise("60+")
    ).withColumn(
        "promo_code_used",
        F.when(F.col("promo_code").isNull(), 0).otherwise(1)
    )

    return enriched_df


def calculate_browsing_time(feature_df: DataFrame,
                            browsing_behaviour_df: DataFrame,
                            browsing_events: list) -> DataFrame:
    """
    Calculate the total browsing time per session for specified browsing events
    and add it as a feature to the feature DataFrame.

    :param feature_df: Spark DataFrame containing session-level features.
    :param browsing_behaviour_df: Spark DataFrame containing browsing behavior data.
                                  Must include 'session_id', 'event_time', and 'event_type' columns.
    :param browsing_events: List of event types to consider as browsing events (e.g., ['SCR', 'SER', 'HP']).
    :return: Enriched feature DataFrame with a new column 'total_browsing_time'.
    """
    browsing_df = browsing_behaviour_df.withColumn("event_time", F.to_timestamp("event_time"))

    window_spec = Window.partitionBy("session_id").orderBy("event_time")

    browsing_df = browsing_df.withColumn(
        "next_event_time",
        F.lag("event_time").over(window_spec)
    ).withColumn(
        "time_spent",
        F.when(F.col("next_event_time").isNull(), None)
         .otherwise(F.col("event_time").cast("long") - F.col("next_event_time").cast("long"))
    )

    # Filter to only include specified browsing events
    browsing_df = browsing_df.filter(F.col("event_type").isin(browsing_events))

    # Calculate total browsing time
    browsing_time_df = browsing_df.groupBy("session_id").agg(
        F.sum("time_spent").alias("total_browsing_time")
    ).fillna(0)

    enriched_feature_df = feature_df.join(browsing_time_df, on="session_id", how="inner")
    return enriched_feature_df

import pandas as pd
import reverse_geocode

def add_city_features(feature_df: pd.DataFrame, lat_col: str, long_col: str) -> pd.DataFrame:
    """
    Adds a 'city' column to the DataFrame by reverse geocoding latitude and longitude.

    Args:
        feature_df (pd.DataFrame): DataFrame containing latitude and longitude columns.
        lat_col (str): Column name for latitude.
        long_col (str): Column name for longitude.

    Returns:
        pd.DataFrame: DataFrame with a new 'city' column added.
    """
    # Ensure lat/long columns exist
    if lat_col not in feature_df.columns or long_col not in feature_df.columns:
        raise ValueError(f"Columns '{lat_col}' and '{long_col}' must exist in the DataFrame.")

    # Convert lat-long pairs into tuples
    lat_long_tuples = list(zip(feature_df[lat_col], feature_df[long_col]))

    # Perform reverse geocoding
    cities = reverse_geocode.search(lat_long_tuples)

    # Extract city names
    feature_df["city"] = [location["city"] for location in cities]

    return feature_df




def preprocess_data(data_paths: Dict[str, str],
                    schemas: Dict[str, StructType],
                    spark: SparkSession) -> DataFrame:
    """
    Preprocess the data by loading, enriching, and adding features to the input datasets.

    :param data_paths: A dictionary containing dataset file paths.
    :param schemas: A dictionary containing schemas for each dataset.
    :param spark: A SparkSession instance.
    :return: A Spark DataFrame containing the fully preprocessed data.
    """
    # Load the data
    dataframes = load_data(data_paths, schemas, spark)

    # Extract transactions and browsing behavior DataFrames
    transactions = dataframes['transactions']
    browsing_behaviour = dataframes['browsing_behaviour']

    print('Preprocessing data...')

    # Add event level ratios and features
    event_levels = add_event_level_ratios(
        add_event_level_features(transactions, browsing_behaviour)
    )

    # Calculate session times
    session_times = calculate_session_times(browsing_behaviour)

    # Enrich data with customer and session information
    enriched_data = enrich_feature_data(event_levels, session_times, dataframes['customer'])

    # Add purchases count
    purchased_count = add_purchases_count(enriched_data, transactions)

    # Add fraud labels
    fraud_labels = add_fraud_labels(purchased_count, data_paths['fraud_transaction'], spark)

    # Add account age, age bins, and promo code usage
    age_bins = add_account_and_promo_features(fraud_labels)

    # Calculate browsing time and finalize
    processed_data = calculate_browsing_time(age_bins, browsing_behaviour, ['SCR', 'SER', 'HP'])


    print("Data preprocessing complete.")
    return processed_data
