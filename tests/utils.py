import pytest
from pyspark.sql import SparkSession

# Initialize a spark context:
@pytest.fixture(scope="session")
def spark_context(request):
    spark = (SparkSession
             .builder
             .appName("python_etl_test")
             .getOrCreate())

    sc = spark.sparkContext

    # teardown
    request.addfinalizer(lambda: spark.stop())

    return sc

def row_to_dict(row):
    """Convert pyspark.Row to dict for easier unordered comparison"""
    return {key: row[key] for key in row.__fields__}


