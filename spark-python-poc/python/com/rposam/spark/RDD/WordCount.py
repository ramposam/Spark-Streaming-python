from pyspark.sql import SparkSession
from com.rposam.spark.config.SparkConf import CreateSparkDriver
import sys
import logging
if __name__ == "__main__":

    logging.basicConfig(filename=r"C:\Users\91889\Desktop\WordCountProgram.log",level=logging.INFO)

    logging.info("Creating Spark Driver ...")
    sparkDriver = CreateSparkDriver()

    logging.info("Spark Driver created Successfully ...")
    sc = sparkDriver.sparkContext
    logging.info("Spark Driver Context initialized ...")

    logging.info("Reading inputs from user ...")
    inputDir = input("Please enter input directory \n")
    outputDir = input("Please enter output directory \n")
    logging.info("Entered input:{}".format(inputDir))
    logging.info("Entered output:{}".format(outputDir))

    wordsRDD = sc.textFile(inputDir)
    logging.info("RDD created after file read")
    words = wordsRDD.flatMap(lambda x: x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    print(type(words))
    words.saveAsTextFile(outputDir)

    logging.info("Output file generated successfully..")
    sparkDriver.stop()
    logging.info("Spark Driver program terminated successfully")