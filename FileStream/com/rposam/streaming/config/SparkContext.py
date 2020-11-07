from pyspark import SparkConf
import configparser as cp


def getSparkConfiguration():
    conf = SparkConf()
    props = cp.ConfigParser()
    props.read("spark.conf")
    for property, value in props.items("SPARK_APP_CONFIGS"):
        conf.set(property, value)
    return conf
