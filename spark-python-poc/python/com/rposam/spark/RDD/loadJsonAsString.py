import logging

from pyspark.sql.functions import from_json,Column as col
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from com.rposam.spark.config.SparkConf import CreateSparkDriver

schema = StructType([
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
if __name__ == "__main__":
    logging.basicConfig(filename=r"C:\Users\91889\Desktop\jsonwithschema.log", level=logging.INFO)
    sparkDriver = CreateSparkDriver()

    file = r"C:\Users\91889\PycharmProjects\pyspark_poc\src\main\python\datasets\randomuserapi.txt"
    df = sparkDriver.read.option("multiline",True).format("json").schema(schema).load(file)
    df.printSchema()
    df.show()

