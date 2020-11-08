from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from com.rposam.spark.config.SparkConf import get_spark_app_config
from com.rposam.spark.config.JDBCConnection import JDBCConnection
import logging

from pyspark.sql.functions import *
import re


def reNameColumns(cols):
    reNamedCols = []
    for col in cols:
        reNamedCols.append(re.sub("[^a-zA-Z0-9]", "_", col.lower()))
    return reNamedCols

if __name__ == "__main__":
    conf = get_spark_app_config()
    sparkDriver =  SparkSession.builder.config(conf=conf).getOrCreate()
    dataFile = input("Please enter input file path \n")
    output_dir = input("Please enter output Directory \n")

    jsonData = sparkDriver.read.format("json").load(dataFile)

    jsonData.printSchema()
    cols = reNameColumns(jsonData.columns)

    jsonDF = jsonData.toDF(*cols).withColumn("id", col("`_id`.`$oid`")).withColumn("earnings_dt", from_unixtime(
        col("earnings_date.`$date`").cast("int"))).drop("_id", "earnings_date")

    jsonDF.printSchema()
    props = JDBCConnection().getPostgresConnectoin()

    # jsonDF.coalesce(1).write.format("jdbc"). \
    #     mode(saveMode="append"). \
    #     options(url="jdbc:postgresql://postgres.cabrrax308iv.ap-south-1.rds.amazonaws.com:5432/awspostgredb",
    #             dbtable="awspostgredb.json_tab",
    #             user="postgresuser",
    #             password="password",
    #             driver="org.postgresql.Driver",
    #             batchsize=25000).save()

    jsonDF.coalesce(1).write.format("jdbc"). \
        mode(saveMode="append").options(**props).option("dbtable", "awspostgredb.json_tab").save()


sparkDriver.stop()
