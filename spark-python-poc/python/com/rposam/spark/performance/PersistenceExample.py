import sys
from datetime import datetime
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,DateType,IntegerType
from pyspark import StorageLevel
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
    sc = spark.sparkContext
    print(sc._jsc.sc().uiWebUrl().get())
    inputDir = sys.argv[1]
    rdd = sc.textFile(inputDir)
    #print(type(rdd))
    #print(rdd.take(10)) # Take is an action
    splitRDD = rdd.map(lambda line: line.split(","))
    #print(type(splitRDD))
    tupleRDD = splitRDD.map(lambda  row : (int(row[0]),row[1],row[2],row[3],datetime.strptime(row[4], '%Y-%m-%d'),float(row[5]),row[6],int(row[7])))
    #print(tupleRDD.take(10))
    print("No of Partitions: {}".format(tupleRDD.getNumPartitions()))
    df = spark.createDataFrame(tupleRDD,empSchema)
    df.persist(StorageLevel.MEMORY_ONLY)
    df.show(10)
    newDF = df.repartition(6)
    print("No of Partitions after repartition: {}".format(newDF.rdd.getNumPartitions()))
    print("No of rows in deptno-10: {}".format(newDF.filter("deptno == 10").count()))
    print("No of rows in deptno-20: {}".format(newDF.filter("deptno == 20").count()))
    print("No of rows in deptno-30: {}".format(newDF.filter("deptno == 30").count()))
    print("No of rows in deptno-40: {}".format(newDF.filter("deptno == 40").count()))
    time.sleep(1000)
