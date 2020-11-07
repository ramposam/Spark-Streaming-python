import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from com.rposam.streaming.log.Log4j import Log4j
from pyspark.sql.functions import window, col, expr,countDistinct

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
        format("csv"). \
        schema(schema). \
        option("maxFilesPerTrigger", 1). \
        load(sys.argv[1])

    df.createOrReplaceTempView("EmpFile")
    groupDF = spark.sql("""
                select deptno,
                    max(sal) as max_sal, 
                    min(sal) as min_sal, 
                    sum(sal) as total_sal, 
                    count(empno) as totalnoof_employees 
                 from EmpFile 
                group by deptno               
                """)
    writer = groupDF. \
        writeStream. \
        format("console"). \
        option("nullValue", None). \
        queryName("EMP File streaming Aggregations"). \
        option("checkpointLocation", "filestream.aggregation.check-point-dir"). \
        option("emptyValue", None). \
        outputMode("update"). \
        option("path", sys.argv[2]). \
        trigger(processingTime="30 seconds"). \
        start()

    logger.info("writer started...")
    writer.awaitTermination()
