import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}
import scala.math


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // import flightData.csv
    val flightsDF = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./flightData.csv")


    //import passengers.csv
    val paxDF = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./passengers.csv")



    val fromDte = "2017-01-01" // Date in yyyy-MM-dd format
    val toDte = "2017-03-31" // Assume inputs
    val formatter = new SimpleDateFormat("yyyy-MM-dd")

      totalFlightsByMonth(flightsDF)
      top100Flyers(flightsDF, paxDF)
      longestRunFlyers(flightsDF)
      flownTogetherThrice(flightsDF)

    try {
      flownTogether(flightsDF, 5, formatter.parse(fromDte), formatter.parse(toDte))  //assumed inputs - N times = 5
    } catch {
      case _: Exception => println() // Return None if parsing fails

    }

    def totalFlightsByMonth(flightsDF: DataFrame): DataFrame = {

      val df = flightsDF
        .withColumn("month", month(flightsDF("date")))
        .groupBy("month")
        .agg(count("month")
          .as("Number Of Flights"))
        .orderBy("month")
      df.show()

      toCSV("TotalFlightsByMonth-Output", df)
      df
    }

    def top100Flyers(flightsDF: DataFrame, paxDF: DataFrame): DataFrame = {

      val paxinfo = paxDF.select("passengerId", "firstName", "lastName")
      val flownDf = flightsDF
        .groupBy("passengerId")
        .agg(count("passengerId")
          .as("Number Of Flights"))
        .orderBy(desc("Number Of Flights"))
        .join(paxinfo, Seq("passengerId"))
        .limit(100) // top 100 flyers

      flownDf.show()
      toCSV("top100Flyers",flownDf)
      flownDf
    }

    def longestRunFlyers(flightsDF: DataFrame): DataFrame = {

      val rows: Array[Row] = flightsDF
        .filter(col("from") =!= col("to"))
        .groupBy("passengerId")
        .agg(collect_list("to"))
        .collect()

      val answers: Array[(Int, Int)] = rows.map { row =>
        (
          row.getInt(0),
          row
            .getAs[Seq[String]](1)
            .takeWhile(!_.equalsIgnoreCase("uk"))
            .size
        )
      }
      //answers.foreach(e => println(s"${e._1} ${e._2}"))
      val answersDF = answers
        .toSeq
        .toDF("Passenger ID", "Longest Run")
        .orderBy(desc("Longest Run"))
      toCSV("LongestRun", answersDF)
      answersDF.show()
      answersDF
    }

    def flownTogetherThrice(flightsDF: DataFrame): Unit = {

      val flownTogetherThriceDf  = getPaxFlownTogetherDF(flightsDF)
        .groupBy("passenger1", "passenger2")
        .agg(count("*") as "Number Of Flights Together")
        .filter($"Number Of Flights Together" > 3)
        .orderBy(desc("Number Of Flights Together")) //since number of flights to be in descending order
      flownTogetherThriceDf.show
       toCSV("FlownTogetherThrice", flownTogetherThriceDf)

    }

    def getPaxFlownTogetherDF(flightDF: DataFrame): DataFrame = {
      val pairs = flightsDF
        .as("pax1")
        .join(flightsDF.as("pax2"), $"pax1.flightId" === $"pax2.flightId")
        .select($"pax1.flightId", $"pax1.passengerId".as("passenger1"), $"pax2.passengerId".as("passenger2"), $"pax2.date".as("date1"))
        .filter(col("passenger1") < col("passenger2"))
      pairs

    }


    def flownTogether(flightsDF: DataFrame, atLeastNTimes: Int, from: Date, to: Date): DataFrame = {

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val fromDte = sdf.format(from)
      println(from + " : from")
      println(to + " : to ")

      val toDte = sdf.format(to)

      val getPaxFlownTogether = getPaxFlownTogetherDF(flightsDF)
        .filter($"date1".lt(lit(toDte)))
        .filter($"date1".gt(lit(fromDte)))
        .groupBy("passenger1", "passenger2")
        .agg(count("*") as "Number Of Flights Together")
        .filter($"Number Of Flights Together" > atLeastNTimes)
        .orderBy(desc("Number Of Flights Together")) //since number of flights to be in descending order
        .withColumn("from",lit(fromDte))
        .withColumn("to",lit(toDte))

       toCSV("flownTogetherNTimes",getPaxFlownTogether)
       getPaxFlownTogether.show()
       getPaxFlownTogether


    }

    def toCSV(csvName:  String , dataFrame: DataFrame): Unit = {
      val outputPath = "./output/"
      dataFrame
        .coalesce(1)
        .write.option("header","true")
        .option("delimiter",",")
        .mode("Overwrite")
        .csv(outputPath+csvName)
    }
  }
}
