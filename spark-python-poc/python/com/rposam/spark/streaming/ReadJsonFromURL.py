import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, Row
from com.rposam.spark.config.SparkConf import get_spark_app_config
from urllib.request import urlopen
import json
import pandas as pd
import requests

if __name__ == "__main__":

    conf = get_spark_app_config()
    sparkDriver = SparkSession.builder.config(conf=conf).getOrCreate()

    jsonDict = requests.get("https://randomuser.me/api/0.8").json()

    response = urlopen("https://randomuser.me/api/0.8")
    jsonDict2 = json.loads(response.read())

    rdd = sparkDriver.sparkContext.parallelize([jsonDict])

    df = sparkDriver.read.json(rdd)

    rdd2 = sparkDriver.sparkContext.parallelize([jsonDict2])
    df2 = sparkDriver.read.json(rdd2)

    df.withColumn("user", col("results.user")).show()
    df2.withColumn("user", col("results.user")).show()
    sparkDriver.stop()
