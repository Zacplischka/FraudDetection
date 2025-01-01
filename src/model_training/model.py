from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline
from src.model_training import preprocessing as pp


def create_ml_pipelines(categorical_columns, numeric_columns, label_col="is_fraud", seed=42):
    """
    Create machine learning pipelines for Random Forest and GBT classifiers.

    :param categorical_columns: List of categorical columns to be indexed and encoded.
    :param numeric_columns: List of numeric columns to be used in the feature vector.
    :param label_col: Name of the label column in the dataset (default: 'is_fraud').
    :param seed: Random seed for reproducibility (default: 42).
    :return: A dictionary containing 'rf_pipeline' (Random Forest) and 'gbt_pipeline' (GBT) pipelines.
    """
    # Create the pipeline stages for preprocessing
    stages = []

    for column in categorical_columns:
        # Add StringIndexer and OneHotEncoder for categorical columns
        indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index", handleInvalid="skip")
        encoder = OneHotEncoder(inputCol=f"{column}_index", outputCol=f"{column}_vec")
        stages += [indexer, encoder]

    # Assemble all features into a single vector
    assembler_inputs = [f"{column}_vec" for column in categorical_columns] + numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages.append(assembler)

    # Create classifiers
    rf = RandomForestClassifier(featuresCol="features", labelCol=label_col, seed=seed)
    gbt = GBTClassifier(featuresCol="features", labelCol=label_col, seed=seed)

    # Create pipelines for both classifiers
    rf_pipeline = Pipeline(stages=stages + [rf])
    gbt_pipeline = Pipeline(stages=stages + [gbt])

    return {
        "rf_pipeline": rf_pipeline,
        "gbt_pipeline": gbt_pipeline
    }


from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def balanced_train_test_split(feature_df: DataFrame,
                              categorical_columns: list,
                              numeric_columns: list,
                              label_col: str = 'is_fraud',
                              non_fraud_fraction: float = 0.2,
                              train_test_split: list = [0.8, 0.2],
                              seed: int = 42) -> tuple:
    """
    Create a balanced train-test split by combining all fraud transactions with a fraction of non-fraud transactions.

    :param feature_df: Spark DataFrame containing the data to be split.
    :param categorical_columns: List of categorical columns to include in the selected features.
    :param numeric_columns: List of numeric columns to include in the selected features.
    :param label_col: The name of the label column (default: 'is_fraud').
    :param non_fraud_fraction: Fraction of non-fraud transactions to include (default: 0.2).
    :param train_test_split: Proportion of train-test split (default: [0.8, 0.2]).
    :param seed: Random seed for reproducibility (default: 42).
    :return: A tuple of two DataFrames: (train_data, test_data).
    """

    print("Starting balanced train-test split...")

    # Filter fraud and non-fraud transactions
    frauds = feature_df.filter(F.col(label_col) == 1)
    non_frauds = feature_df.filter(F.col(label_col) == 0)

    # Combine all fraud transactions with a fraction of non-fraud transactions
    combined_df = frauds.union(non_frauds.sample(fraction=non_fraud_fraction, seed=seed))

    # Select only the relevant features
    selected_features = categorical_columns + numeric_columns + [label_col]
    combined_df = combined_df.select(selected_features)

    # Perform train-test split
    train_data, test_data = combined_df.randomSplit(train_test_split, seed=seed)

    print("Balanced train-test split completed.")

    return train_data, test_data

def preprocess_and_train(
    data_paths: dict,
    schemas: dict,
    spark,
    categorical_columns: list,
    numeric_columns: list,
    non_fraud_fraction: float = 0.2,
    train_test_split_ratio: list = [0.8, 0.2],
    model_save_path: str = "src/model_training/gb_model",
    seed: int = 42
):
    """
    Preprocess the data, train machine learning models, and save the best model.

    :param data_paths: Dictionary containing paths to the datasets.
    :param schemas: Dictionary containing schemas for the datasets.
    :param spark: SparkSession instance.
    :param categorical_columns: List of categorical columns for feature engineering.
    :param numeric_columns: List of numeric columns for feature engineering.
    :param non_fraud_fraction: Fraction of non-fraud transactions to include in the balanced dataset (default: 0.2).
    :param train_test_split_ratio: List specifying the train-test split ratio (default: [0.8, 0.2]).
    :param model_save_path: Path to save the best model (default: "src/model_training/gb_model").
    :param seed: Random seed for reproducibility (default: 42).
    :return: Trained models (Random Forest and GBT) and the best model (GBT).
    """
    # Step 1: Preprocess the data
    processed_data = pp.preprocess_data(data_paths, schemas, spark)

    # Step 2: Create a balanced train-test split
    train_data, test_data = balanced_train_test_split(
        processed_data,
        categorical_columns,
        numeric_columns,
        non_fraud_fraction=non_fraud_fraction,
        train_test_split=train_test_split_ratio,
        seed=seed
    )

    # Step 3: Create ML pipelines
    pipelines = create_ml_pipelines(categorical_columns, numeric_columns)

    # Extract Random Forest and GBT pipelines
    rf_pipeline = pipelines["rf_pipeline"]
    gbt_pipeline = pipelines["gbt_pipeline"]

    # Step 4: Train the models
    print('Training Random Forest model...')
    rf_model = rf_pipeline.fit(train_data)
    print('Training GBT model...')
    gbt_model = gbt_pipeline.fit(train_data)

    # Step 5: Save the best model (GBT)
    print(f"Saving the best model to {model_save_path}")
    gbt_model.write().overwrite().save(model_save_path)

    return rf_model, gbt_model
