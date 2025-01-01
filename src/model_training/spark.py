from pyspark.sql import SparkSession
from pyspark import SparkConf

def create_spark_session(app_name="MySparkApp",
                         max_partition_bytes="12000000",
                         master="local[*]",
                         executor_memory="16g",
                         driver_memory="16g",
                         adaptive_enabled=True,
                         log_level="ERROR",
                         jdbc_driver_path="/path/to/postgresql-<version>.jar"):
    """
    Create and configure a SparkSession.

    :param app_name: Name of the Spark application.
    :param max_partition_bytes: Maximum size of a partition in bytes for Spark SQL files.
    :param master: Spark master URL (e.g., 'local[*]' for all available cores).
    :param executor_memory: Memory allocated to Spark executors.
    :param driver_memory: Memory allocated to the Spark driver.
    :param adaptive_enabled: Enable or disable adaptive query execution.
    :param log_level: Logging level for SparkContext.
    :param jdbc_driver_path: Path to the PostgreSQL JDBC driver JAR file.
    :return: A configured SparkSession instance.
    """

    # Create SparkConf object
    conf = SparkConf() \
        .setAppName(app_name) \
        .set("spark.sql.files.maxPartitionBytes", max_partition_bytes) \
        .setMaster(master) \
        .set("spark.executor.memory", executor_memory) \
        .set("spark.driver.memory", driver_memory) \
        .set("spark.sql.shuffle.partitions", 4) \
        .set("spark.jars", jdbc_driver_path)  # Add JDBC driver path

    # Initialize the Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Configure adaptive query execution if enabled
    if adaptive_enabled:
        spark.conf.set("spark.sql.adaptive.enabled", "true")

    # Set log level for SparkContext
    spark.sparkContext.setLogLevel(log_level)

    # Clear Spark catalog cache
    spark.catalog.clearCache()

    print("Spark session created successfully.")

    return spark
