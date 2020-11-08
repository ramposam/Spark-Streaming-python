from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from com.rposam.spark.config.JDBCConnection import JDBCConnection
from pyspark.sql import functions as f, DataFrame, SparkSession

from com.rposam.spark.config.SparkConf import get_spark_app_config

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

props = JDBCConnection().getPostgresConnectoin()


def witeToJDBC(empDF):
    empDF.coalesce(1).write.format("jdbc"). \
        mode(saveMode="append").options(**props).option("dbtable", "awspostgredb.randomuser_tab").save()


def processDF(df: DataFrame):
    df1 = df.withColumn("rslts", explode(f.col("results"))). \
        withColumn("gender", f.col("rslts.user.gender")). \
        withColumn("title", f.col("rslts.user.name.title")). \
        withColumn("first_name", f.col("rslts.user.name.first")). \
        withColumn("last_name", f.col("rslts.user.name.last")). \
        withColumn("street", f.col("rslts.user.location.street")). \
        withColumn("city", f.col("rslts.user.location.street")). \
        withColumn("state", f.col("rslts.user.location.state")). \
        withColumn("zip", f.col("rslts.user.location.zip")). \
        withColumn("email", f.col("rslts.user.email")). \
        withColumn("username", f.col("rslts.user.username")). \
        withColumn("password", f.col("rslts.user.password")). \
        withColumn("salt", f.col("rslts.user.salt")). \
        withColumn("md5", f.col("rslts.user.md5")). \
        withColumn("sha1", f.col("rslts.user.sha1")). \
        withColumn("sha256", f.col("rslts.user.sha256")). \
        withColumn("registered", f.col("rslts.user.registered")). \
        withColumn("dob", f.col("rslts.user.dob")). \
        withColumn("phone", f.col("rslts.user.phone")). \
        withColumn("cell", f.col("rslts.user.cell")). \
        withColumn("large_picture", f.col("rslts.user.picture.large")). \
        withColumn("medium_picture", f.col("rslts.user.picture.medium")). \
        withColumn("thumbnail_picture", f.col("rslts.user.picture.thumbnail")) \
        .drop("rslts", "results")
    df1.show()
    witeToJDBC(df1)


if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic1") \
        .option("enable.auto.commit", True) \
        .load().selectExpr("CAST(value AS STRING) as jsonData") \
        .select(from_json("jsonData", schema).alias("data")) \
        .select("data.*")
    df.printSchema()
    df.writeStream.format("console").outputMode("update").trigger(processingTime="10 seconds").foreachBatch(
        lambda batchDF, batchId: processDF(batchDF)
    ).start().awaitTermination()
