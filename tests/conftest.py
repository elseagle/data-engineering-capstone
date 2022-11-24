import pytest


@pytest.fixture(scope="session")
def total_leads():
    """Fixture to load data from a file."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "Spark job for tests"
    ).getOrCreate()

    return (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("parquet")
        .load("output/total_leads_gotten_by_year.parquet")
    ), spark


@pytest.fixture(scope="session")
def last_10_days():
    """Fixture to load data from a file."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "Spark job for tests"
    ).getOrCreate()

    return (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("parquet")
        .load("output/last_10_days.parquet")
    ), spark


@pytest.fixture(scope="session")
def hq_staffs():
    """Fixture to load data from a file."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "Spark job for tests"
    ).getOrCreate()

    return (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("parquet")
        .load("output/hq_staffs.parquet")
    ), spark


@pytest.fixture(scope="session")
def leads_per_country():
    """Fixture to load data from a file."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "Spark job for tests"
    ).getOrCreate()

    return (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .format("parquet")
        .load("output/leads_per_country.parquet")
    ), spark