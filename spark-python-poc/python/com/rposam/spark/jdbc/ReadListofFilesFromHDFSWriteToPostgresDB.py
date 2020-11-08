import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
import psycopg2
import os
import sys
import configparser as cp
import pydoop.hdfs as hdfs


class postgresCopyOverSparkDFWrite:

    def time_test(func):
        t1 = time.time()
        func()
        t2 = time.time()
        print("==============================================================")
        print(str(t2 - t1) + ": " + func.__name__)
        return None

    # define a function
    # @time_test
    def execute_copy(dbname, user, password, host, port, input_path, table_name):
        con = psycopg2.connect(database=dbname, user=user, password=password, host=host,
                               port=port)
        cursor = con.cursor()
        hadoop = sc._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem
        conf = hadoop.conf.Configuration()
        path = hadoop.fs.Path('/input_multi_files/')
        for f in hadoop.fs.FileSystem.get(conf).listStatus(path):
            filename = f.getPath().toString()
            relativePath= filename[filename.index("/input_multi_files"):]
            print(relativePath)
            with hdfs.open(relativePath) as file:
                cursor.copy_from(file, 'awspostgredb.temp',sep=",")
                file.close()
        con.commit()
        con.close()

    # @time_test
    def write(df, url, table_name, user, password, driver):
        df.write.format("jdbc"). \
            mode(saveMode="append"). \
            option("url", url). \
            option("dbtable", table_name). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            option("batchsize", 25000). \
            save()

    def getSparkDF(input_path, maven_packages):
        spark = SparkSession.builder. \
            config("spark.jars.packages", maven_packages). \
            master("local[*]").appName("pyspark postgre performance test"). \
            getOrCreate()
        schema = StructType() \
            .add('a', IntegerType()) \
            .add('b', IntegerType())
        df = spark.read.format("csv").load(input_path, schema=schema, inferSchema=False)
        print("No of Partitions:{0}".format(df.rdd.getNumPartitions()))
        return df
        # df.repartition(10).write.option("maxRecordsPerFile", 50000).mode("overwrite").csv(r"C:\Users\91889\Desktop\Spark-Training\output")
    if __name__ == "__main__":
        conf_path = sys.argv[1]
        input_path = sys.argv[2]
        config = cp.ConfigParser()
        config.read(conf_path)
        host = config.get("postegres_props", "host")
        port = config.get("postegres_props", "port")
        user = config.get("postegres_props", "user")
        password = config.get("postegres_props", "password")
        dbname = config.get("postegres_props", "dbname")
        table_name = config.get("postegres_props", "table_name")
        driver = config.get("postegres_props", "driver")
        # args = [(i, i + 1) for i in range(1, 1 * 10 ** 6, 2)]
        url = "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)
        print(url)
        maven_packages = config.get("postegres_props", "maven_packages")
        df = getSparkDF(input_path, maven_packages)
        t1 = time.time()
        #write(df, url, table_name, user, password, driver)
        t2 = time.time()
        print(str(t2 - t1) + ": write")
        t1 = time.time()
        execute_copy(dbname, user, password, host, port, input_path, table_name)
        t2 = time.time()
        print(str(t2 - t1) + ": execute_copy")
