from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from python_etl import example_job

if __name__ == "__main__":
    conf = SparkConf().setAppName('python_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    example_job.etl_job(sc, sqlContext)
