import sys
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, split, expr, window
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
    # df = spark. \
    #     read. \
    #     format("csv"). \
    #     schema(schema). \
    #     option("path", sys.argv[1]). \
    #     load()
    
    # Read delta file to verify the updates are correct
    df = spark. \
        read. \
        format("delta"). \
        schema(schema). \
        option("path", sys.argv[2]). \
        load()
    df.show()
    
    logger.info("File reading started...")
    # write as delta file
    # df.write.format("delta").mode("append").save(sys.argv[2])
    
    # implement acid properties on delta file
    
    # deltaTable = DeltaTable.forPath(spark, sys.argv[2])
    # deltaTable.alias("oldData") \
    #     .merge(
    #     df.alias("newData"),
    #     "oldData.empno = newData.empno") \
    #     .whenMatchedUpdate(set={"empno": col("newData.empno"),
    #                             "ename": col("newData.ename"),
    #                             "job": col("newData.job"),
    #                             "mgr": col("newData.mgr"),
    #                             "hiredate": col("newData.hiredate"),
    #                             "sal": col("newData.sal"),
    #                             "comm": col("newData.comm"),
    #                             "deptno": col("newData.deptno")}) \
    #     .whenNotMatchedInsert(values={"empno": col("newData.empno"),
    #                                   "ename": col("newData.ename"),
    #                                   "job": col("newData.job"),
    #                                   "mgr": col("newData.mgr"),
    #                                   "hiredate": col("newData.hiredate"),
    #                                   "sal": col("newData.sal"),
    #                                   "comm": col("newData.comm"),
    #                                   "deptno": col("newData.deptno")}) \
    #     .execute()
    
    # Delete from delta file when condition matched 
    # deltaTable = DeltaTable.forPath(spark, sys.argv[2])
    # deltaTable.alias("oldData") \
    #     .merge(
    #     df.alias("newData"),
    #     "oldData.empno = newData.empno") \
    #     .whenMatchedDelete() \
    # .execute()
    
    spark.stop()
    logger.info("application stopped")
