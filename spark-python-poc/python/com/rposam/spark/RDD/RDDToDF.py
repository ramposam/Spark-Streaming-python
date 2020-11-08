from com.rposam.spark.config.SparkConf import CreateSparkDriver
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import logging


def fillMissingFields(line, maxsize):
    cols = []
    for i in range(maxsize):
        try:
            cols.append(line[i])
        except:
            cols.append(None)
    return cols


schema = StructType([
    StructField("c0", StringType()),
    StructField("c1", StringType()),
    StructField("c2", StringType()),
    StructField("c3", StringType()),
    StructField("c4", StringType()),
    StructField("c5", StringType()),
    StructField("c6", StringType()),
    StructField("c7", StringType()),
    StructField("c8", StringType()),
    StructField("c9", StringType()),
    StructField("c10", StringType())
])
if __name__ == "__main__":
    logging.basicConfig(filename=r"C:\Users\91889\Desktop\rddtodf.log", level=logging.INFO)
    sparkDriver = CreateSparkDriver()
    logging.info("Spark Driver Created")
    input_dir = r"C:\Users\91889\PycharmProjects\pyspark_poc\src\main\python\datasets\rddsample.txt"

    logging.info("Input file loaded:{}".format(input_dir))
    rdd = sparkDriver.sparkContext.textFile(input_dir)

    logging.info("file loaded as rdd of type default:{}".format(type(rdd)))
    rdd1 = rdd.map(lambda line: line.split(" "))

    logging.info("After split of rdd type:{}".format(type(rdd1)))
    logging.info("no of elements in rdd:{}".format(rdd1.map(lambda line: len(line)).toLocalIterator()))
    logging.info("Elements in rdd:{}".format(rdd1.toLocalIterator()))

    maxElementsize = max(rdd1.map(lambda line: len(line)).toLocalIterator())
    rdd2 = rdd1.map(lambda line: fillMissingFields(line, maxElementsize))

    logging.info("After filling missing fields rdd:{}".format(rdd2.toLocalIterator()))
    df = rdd2.toDF(schema)
    df.show()
