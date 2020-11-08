import io
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,ArrayType

from com.rposam.spark.config.SparkConf import get_spark_app_config
from kafka import KafkaConsumer

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

schema =  StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("user", StructType([
                StructField("gender", StringType()),
                StructField("name", StructType([
                    StructField("title", StringType()),
                    StructField("first", StringType()),
                    StructField("last", StringType())
                ])),
                StructField("location", StructType([
                    StructField("street", StringType()),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("zip", StringType())
                ])),
                StructField("email", StringType()),
                StructField("username", StringType()),
                StructField("password", StringType()),
                StructField("salt", StringType()),
                StructField("md5", StringType()),
                StructField("sha1", StringType()),
                StructField("sha256", StringType()),
                StructField("registered", StringType()),
                StructField("dob", StringType()),
                StructField("phone", StringType()),
                StructField("cell", StringType()),
                StructField("picture", StructType([
                    StructField("large", StringType()),
                    StructField("medium", StringType()),
                    StructField("thumbnail", StringType())
                ]))
            ]))
        ])
    )),
    StructField("nationality", StringType()),
    StructField("seed", StringType()),
    StructField("version", StringType())
])
def convertRdd(rdd):
    #print(rdd.take(5))
    if rdd.count() > 0:
        data = []
        for (key, value) in rdd.toLocalIterator():
            data.append(value)
            # rdd = spark.sparkContext.parallelize([value])
            # df = spark.read.schema(schema).json(rdd)
            # df = spark.read.json(rdd).toDF(schema) -- Fails
        rdd1 = spark.sparkContext.parallelize(data)
        df = spark.read.schema(schema).json(rdd1)
        df.show()


if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 5)
    directKafkaStream = KafkaUtils.createDirectStream(ssc, ["topic2"], {"bootstrap.servers": 'localhost:9092'})

    print("STREAM>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    directKafkaStream.foreachRDD(convertRdd)
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    ssc.start()
    ssc.awaitTermination()
