from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

from com.rposam.spark.config.SparkConf import get_spark_app_config

empSchema = StructType([
    StructField("empno", IntegerType()),
    StructField("ename", StringType()),
    StructField("job", StringType()),
    StructField("mgr", StringType()),
    StructField("hiredate", DateType()),
    StructField("sal", DoubleType()),
    StructField("comm", StringType()),
    StructField("deptno", IntegerType()),
])

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    df = spark.readStream.format("csv").schema(empSchema).load(
        r"C:\Users\91889\Desktop\Spark-Training\datasets\StreamInputEmp")
    df.printSchema()
    df.selectExpr("CAST('1' AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "topic2") \
        .option("enable.auto.commit", True) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", r"C:\Users\91889\Desktop\checkpointDir") \
        .start() \
        .awaitTermination()
