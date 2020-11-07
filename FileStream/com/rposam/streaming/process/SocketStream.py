import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import col,split,expr,window
from com.rposam.streaming.log.Log4j import Log4j

from com.rposam.streaming.config.SparkContext import getSparkConfiguration

schema = StructType([
    StructField("empno", IntegerType()),
    StructField("ename", StringType()),
    StructField("job", StringType()),
    StructField("mgr", IntegerType()),
    StructField("hiredate", DateType()),
    StructField("sal", DoubleType()),
    StructField("comm", DoubleType()),
    StructField("deptno", IntegerType())])


def writeLog(BatchDF, BatchId):
    logger.info(f"Batch Id: {BatchId}" + str(BatchDF))


if __name__ == "__main__":
    conf = getSparkConfiguration()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    logger.info("Streaming Data from file started...")
    df = spark. \
        readStream. \
        format("socket"). \
        option("host", "localhost"). \
        option("port", "9999"). \
        load()

    df.printSchema()
    valueDF = df.selectExpr("CAST(value as STRING) as data").\
        withColumn("row", split("data", ",")). \
        withColumn("empno", expr("row").getItem(0)). \
        withColumn("ename", expr("row").getItem(1)). \
        withColumn("job", expr("row").getItem(2)). \
        withColumn("mgr", expr("row").getItem(3)). \
        withColumn("hiredate", expr("row").getItem(4)). \
        withColumn("sal", expr("row").getItem(5)). \
        withColumn("comm", expr("row").getItem(6)). \
        withColumn("deptno", expr("row").getItem(7)). \
        drop("row", "data")


    writer = valueDF. \
        writeStream. \
        format("console"). \
        option("nullValue", None). \
        queryName("EMP socket streaming"). \
        option("checkpointLocation", "socketstream.check-point-dir"). \
        option("emptyValue", None). \
        outputMode("append"). \
        option("path", sys.argv[1]). \
        trigger(processingTime="10 seconds"). \
        start()

    logger.info("Writer started...")
    writer.awaitTermination()
