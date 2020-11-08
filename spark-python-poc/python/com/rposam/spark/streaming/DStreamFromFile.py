from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,LongType,DateType,DoubleType,StringType

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from com.rposam.spark.config.SparkConf import get_spark_app_config

empSchema = StructType([
    StructField("empno", LongType()),
    StructField("ename", StringType()),
    StructField("job", StringType()),
    StructField("mgr", StringType()),
    StructField("hiredate", StringType()),
    StructField("sal", DoubleType()),
    StructField("comm", StringType()),
    StructField("deptno", LongType()),
])
# Documentation
def ConvertToDF(rddContent):
    if rddContent.count()>0 :
        # RDD has more no of rows
        # So , first split each row by \n, and then loop through each element
        # Then split each row by delimiter ,
        linesRDD = rddContent.map(lambda line: line.split('\n'))
        rows = []
        for ele in linesRDD.collect():
            for row in ele:
                print("row: {}".format(row))
                rowList = row.split(",")
                tuple = (int(rowList[0]), rowList[1], rowList[2], rowList[3], rowList[4], float(rowList[5]), rowList[6], int(rowList[7]))
                rows.append(tuple)
        df = spark.createDataFrame(rows,empSchema)
        df.show()
    else:
        print("RDD is empty")


if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream(hostname="localhost",port=1234)
    #lines = ssc.textFileStream(r"C:\Users\91889\Desktop\Spark-Training\datasets\output")
    lines.foreachRDD(lambda linesrdd: ConvertToDF(linesrdd))

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
