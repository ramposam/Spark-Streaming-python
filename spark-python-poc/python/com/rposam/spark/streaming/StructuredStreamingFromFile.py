import  sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql import functions as f
from com.rposam.spark.config.JDBCConnection import JDBCConnection
from com.rposam.spark.config.SparkConf import get_spark_app_config
from pyspark.sql import SparkSession
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

props = JDBCConnection().getPostgresConnectoin()
def getEachBatchAggregatedResults(batchDF):
    batchDF.groupBy("deptno").agg(f.countDistinct(f.Column("empno"))).alias("dept_cnt").show()

def witeToJDBC(empDF):
    empDF.coalesce(1).write.format("jdbc"). \
        mode(saveMode="append").options(**props).option("dbtable", "spark.emp").save()

if __name__ == "__main__":
    conf = get_spark_app_config()
    sparkDriver = SparkSession.builder.config(conf=conf).getOrCreate()
    inputStreamDir = sys.argv[1]
    outputStreamDir = sys.argv[2]
    checkPointDir = sys.argv[3]
    df = sparkDriver \
        .readStream \
        .format("csv") \
        .schema(empSchema) \
        .load(inputStreamDir)
    df.printSchema()

    # Write dataframe results to console
    df.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode('update') \
        .format("console")\
        .start()

    # Write dataframe results to a file
    df.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode('append') \
        .format("csv") \
        .option("checkpointLocation",checkPointDir)\
        .option("header",True)\
        .start(outputStreamDir)

    # Write Data frame each batch  aggregation results to a console
    df.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode('update') \
        .format("console") \
        .foreachBatch(
        lambda batchDF, batchId: getEachBatchAggregatedResults(batchDF)) \
        .start()

    # Write Data frame each batch  results to a jdbc
    df.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode('update') \
        .format("console") \
        .foreachBatch(
        lambda batchDF, batchId: witeToJDBC(batchDF)) \
        .start().awaitTermination()
