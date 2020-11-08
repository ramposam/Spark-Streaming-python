from com.rposam.spark.config.SparkConf import CreateSparkDriver
from pyspark.sql import functions as f
import logging


def loadTeam(sparkDriver, input_dir):
    print(input_dir + "\Team.csv")
    teamDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Team.csv")
    return teamDF


def loadSeason(sparkDriver, input_dir):
    seasonDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Season.csv")
    return seasonDF


def loadPlayer(sparkDriver, input_dir):
    playerDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Player.csv")
    return playerDF


def loadPlayerMatch(sparkDriver, input_dir):
    playerMatchDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Player_Match.csv")
    return playerMatchDF


def loadMatch(sparkDriver, input_dir):
    matchDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Match.csv")
    return matchDF


def loadBallByBall(sparkDriver, input_dir):
    ballByBallDF = sparkDriver.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_dir + "\Ball_by_Ball.csv")
    return ballByBallDF


if __name__ == "__main__":

    logging.basicConfig(filename=r"C:\Users\91889\Desktop\IPLProgram.log", level=logging.INFO)
    logging.info("Creating Spark Driver Program")
    sparkDriver = CreateSparkDriver()

    logging.info("Reading input directory from user")
    inputDir = input("Please Enter Input Directory to Read \n")

    logging.info("Entered directory name : {}".format(inputDir))
    TeamDF = loadTeam(sparkDriver, inputDir)
    logging.info("loaded TeamDF Successfully...")

    SeasonDF = loadSeason(sparkDriver, inputDir)
    logging.info("loaded SeasonDF Successfully...")

    MatchDF = loadMatch(sparkDriver, inputDir)
    logging.info("loaded MatchDF Successfully...")

    PlayerMatchDF = loadPlayerMatch(sparkDriver, inputDir)
    logging.info("loaded PlayerMatchDF Successfully...")

    BallByBallDF = loadBallByBall(sparkDriver, inputDir)
    logging.info("loaded BallByBallDF Successfully...")

    PlayerDF = loadPlayer(sparkDriver, inputDir).dropna("all")
    logging.info("loaded PlayerDF ...")

    logging.info("Join between  TeamDF and PlayerMatchDF")
    TeamMatchdf = TeamDF.join(PlayerMatchDF, TeamDF.Team_Id == PlayerMatchDF.Team_Id, "INNER"). \
        drop(PlayerMatchDF.Team_Id)

    TeamDF.write.saveAsTable(name="Team",format="avro")
    PlayerDF.write.saveAsTable("Player","parquet")
    PlayerMatchDF.write.saveAsTable("PlayerMatch","json")
    BallByBallDF.write.saveAsTable("BallByBall","orc")
    MatchDF.write.saveAsTable("Match","text")
    SeasonDF.write.saveAsTable("Season","csv")

    logging.info("Join between  TeamMatchdf and PlayerDF")
    TeamMatchdf.join(PlayerDF, TeamMatchdf.Player_Id == PlayerDF.Player_Id).drop(PlayerDF.Player_Id) \
        .withColumn("Captain", f.when(f.col("Is_Captain") == 1, "Yes").otherwise("No")).drop("Is_Captain") \
        .withColumn("Keeper", f.when(f.col("Is_Keeper") == 1, "Yes").otherwise("No")).drop("Is_Keeper") \
        .withColumn("DateOfBirth", f.date_format(f.col("DOB"), "yyyy-MM-dd")) \
        .show(truncate=False)

    logging.info("Terminating Driver Program")
    sparkDriver.stop()
    logging.info("Program completed Successfully")
