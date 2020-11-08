from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser as cp
def get_spark_app_config():
    spark_conf = SparkConf()
    config = cp.ConfigParser()
    config.read("com/rposam/spark/config/spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf
    # driver = SparkSession. \
    #     builder.master("local[*]"). \
    #     config("spark.jars.packages", "org.postgresql:postgresql:42.2.10," +
    #            "org.apache.spark:spark-streaming-kafka-assembly_2.11:1.6.3," +
    #            "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," +
    #            "org.apache.kafka:kafka-clients:2.4.0," +
    #            "org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4"). \
    #     appName("Spark Driver Program").getOrCreate()

    # return driver
