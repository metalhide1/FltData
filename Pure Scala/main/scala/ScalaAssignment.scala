//> using jvm 8
//> using scala 2.12
//> using dep org.apache.spark::spark-core:2.4.8
//> using dep org.apache.spark::spark-sql:2.4.8

import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class FlightDataInfo(passengerId: Int, flightId: Int, to: String, from: String, date: Timestamp)
case class PaxDataInfo(passengerId: Int, firstName: String, lastName: String)

object Assignment {
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

 flightsDF.printSchema()

    //import passengers.csv
    val paxDF = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./passengers.csv")

paxDF.printSchema()

    val fromDte = "2017-01-01" // Date in yyyy-MM-dd format for Q4
    val toDte = "2017-03-31" // Assume inputs for Q4
    val formatterLocalDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val flightDS = flightsDF.as[FlightDataInfo]
    val paxDS = paxDF.as[PaxDataInfo]
    val nTimes = 5 //Assume input for Q4

    val  flightDataInfoList = flightDS.collect().toList
    val paxDataInfoList = paxDS.collect().toList

    spark.close() // Close Spark session

    totalFlightsByMon(flightDataInfoList)
    top100FlyerNames(flightDataInfoList, paxDataInfoList)
    longestRunFlyerIds(flightDataInfoList)
    flyersFlownThrice(flightDataInfoList)
    flownTogetherNTimes(flightDataInfoList, nTimes, fromDte, toDte)


    def totalFlightsByMon(flightDataInfoList: List[FlightDataInfo]) : Seq[(Int,Int)] = {

      val outputHdrs = Seq("Month", "Number Of Flights")

      val fltsByMonth: List[Int] = flightDataInfoList
        .map { flightDS =>
          flightDS
            .date.toLocalDateTime.getMonthValue
        }

      val flts = flightDataInfoList
        .map { flt => (flt.flightId, flt.date.toLocalDateTime.getMonthValue)
    }
        .distinct
        .groupBy(_._2)
        .mapValues(_.size)
        .toSeq.
        sortBy(_._1)

     println("with distinct " + flts)

      val totalFltsByMonth = fltsByMonth
        .groupBy(identity)
        .mapValues(_.size)
        .toSeq.
        sortBy(_._1)
      //Write outputs
      println("************** Question 1 **************************")
      println("Find the total number of flights for each month" )
      println("************** Output 1 Starts *********************")
      println(f"${"Month"}%-10s ${"Number Of Flights"}%-10s")
      totalFltsByMonth.foreach { case (month, totalflights) =>
        println(f"$month%-10d $totalflights%-10d")
      }
      println("\n************** Output 1 Ends *************************\n")

      writeOutputsToCsv("output-TotalFlightsByMonth.csv", totalFltsByMonth, outputHdrs )
      totalFltsByMonth
    }

    def top100FlyerNames(flightData: List[FlightDataInfo], paxData: List[PaxDataInfo]): Seq[(Int,Int,String,String)] = {

      val outputHdrs = Seq("Passenger ID,Number Of Flights,First Name,Last Name")

      val flyerIds : List [Int] = flightData.map ( flightDS =>
        flightDS.passengerId
      )

      val flownCount : Seq[(Int, Int)] = flyerIds
        .groupBy(identity)
        .mapValues(_.size)
        .toSeq.
        sortBy(_._2) (scala.Ordering[Int].reverse)
        .slice(0,100)

      val flyerNames : Seq [(Int,Int,String,String)] = flownCount.flatMap {  case (id, flownCnt) =>
        paxData.filter  { pax => pax.passengerId == id }
          .map { pax =>
            (id, flownCnt, pax.firstName, pax.lastName)
          }
      }

      // Write outputs

      println("\n************** Question 2 **************************\n")
      println("Find the names of the 100 most frequent flyers" )
      println("\n************** Output 2 Starts *********************\n")
      println(f"${"PassengerID"}%-10s ${"Number Of Flights"}%-10s ${"First Name"}%-15s ${"Last Name"}%-10s")
      flyerNames.foreach { case (id, flownCnt, fN, lN) =>
        println(f"$id%-15d $flownCnt%-15d $fN%-15s $lN%-25s")

      }
      println("************** Output 2 Ends **************************")
      writeOutputsToCsv("output-top100TopFlyerNames.csv", flyerNames, outputHdrs )
      flyerNames
    }

    def longestRunFlyerIds(flightData: List[FlightDataInfo]): List[(Int,Int)] = {
      val outputHdrs = Seq ("Passenger ID","Longest Run")
      val destListByPax =
        flightData
          .filter( destByPax => destByPax.to != destByPax.from )
          .map { destByPax =>
            ( destByPax.passengerId, destByPax.to )
          }
      val grpDestListByPax = destListByPax.groupBy(_._1)
        .map { case (paxid, listOfTo) =>
          val listOfDestinations = listOfTo
            .map { case (_,listOfTo) => listOfTo }
          ( paxid, listOfDestinations )
        }
      val longestRunList: List[(Int, Int)] = grpDestListByPax.
        toList.map { grpdPax =>
          (grpdPax._1, grpdPax._2
            .takeWhile(!_.equalsIgnoreCase("uk"))
            .size)
        }.sortBy(_._2)(scala.Ordering[Int].reverse)
      // Write Outputs
      println("\n************************** Question 3 ********************************************\n")
      println("Find the greatest number of countries a passenger has been in without being in the UK" )
      println("\n************************* Output 3 Starts *******************************************\n")
      println(f"${"Passenger ID"}%-15s ${"Longest Run"}%-15s")
      longestRunList.foreach { case (pax1, longestRun) =>
        println(f"$pax1%-15d $longestRun%-15d ")
      }
      println("\n*************************** Output 3 Ends *****************************************\n")
      writeOutputsToCsv("output-longestRunFlyerIds.csv", longestRunList, outputHdrs)
      longestRunList
    }


    def flyersFlownThrice(flightData: List[FlightDataInfo]): Seq[(Int,Int,Int)] = {

      val outputHdrs = Seq("Passenger ID 1,Passenger ID 2,Number Of Flights Together")

      val pairs = flightData.flatMap { fltinfo1 =>
        flightData.filter { fltinfo2 =>
          fltinfo1.flightId == fltinfo2.flightId && fltinfo1.passengerId < fltinfo2.passengerId
        }.map { fltinfo2 =>
          (fltinfo1.passengerId,fltinfo2.passengerId)
        }
      }
      println("completed self join")

      val pairsWithCount = pairs
        .groupBy(identity)
        .mapValues(_.size)
        .toSeq

      // using flatMap
      val pairsWithCountGt3Sorted : Seq[(Int,Int,Int)]= pairsWithCount
        .flatMap { case ((pax1,pax2),flownCnt) =>
        if (flownCnt>3) Some((pax1,pax2,flownCnt)) else None
      }.sortBy(_._3)(scala.Ordering[Int].reverse)

      // Write Outputs
      println("\n********************* Question 4 **************************************\n")
      println("Find the passengers who have been on more than 3 flights together." )
      println("\n********************* Output 4 Starts *********************************\n")
      println(f"${"Passenger1 ID"}%-15s ${"Passenger 2 ID"}%-15s ${"Number Of Flights Together"}%-15s")
      pairsWithCountGt3Sorted.slice(0,20).foreach { case (pax1, pax2, flown) =>
        println(f"$pax1%-15d $pax2%-15d $flown%-15s ")
      }
      println("\n******************** Output 4 Ends ************************************\n")
      writeOutputsToCsv("output-flyersFlownThrice.csv", pairsWithCountGt3Sorted, outputHdrs)
      pairsWithCountGt3Sorted
    }

    def flownTogetherNTimes(flightData: List[FlightDataInfo], atLeastNTimes: Int, from: String, to: String): Seq[(Int,Int,Int, String, String)] = {
      val outputHdrs = Seq("Passenger ID 1,Passenger ID 2,Number Of Flights Together,From Date,To Date")
      val pairsBetweenDates = flightData.flatMap { fltinfo1 =>
        flightData.filter { fltinfo2 =>
          fltinfo1.flightId == fltinfo2.flightId &&
            fltinfo1.passengerId < fltinfo2.passengerId &&
            fltinfo2.date.toLocalDateTime.toLocalDate.isBefore(LocalDate.parse(to,formatterLocalDateTime)) &&
            fltinfo2.date.toLocalDateTime.toLocalDate.isAfter(LocalDate.parse(from,formatterLocalDateTime))
        }.map { fltinfo2 =>
          (fltinfo1.passengerId,fltinfo2.passengerId)
        }
      }

      val pairsBetweenDatesWithCount = pairsBetweenDates
        .groupBy(identity)
        .mapValues(_.size)
        .toSeq

      val pairsWithCountNTimes : Seq[(Int,Int,Int,String,String)]= pairsBetweenDatesWithCount.flatMap {
        case (( paxid1, paxid2 ), flown ) =>
          if ( flown > atLeastNTimes ) Some(( paxid1, paxid2, flown, from, to )) else None
      }.sortBy(_._3)(scala.Ordering[Int].reverse)

      //Write Outputs
      println("\n************************* Question 4.1 *******************************************************\n")
      println("Find the passengers who have been on more than N (5) flights together within the range (from,to)." )
      println("\n-------------------------> N = 5 <--> From = 2017-01-01 <--> To = 2017-03-31 <---------------\n" )
      println("\n************************* Output 4.1 Starts **************************************************\n")

      println(f"${"Passenger1 ID"}%-15s ${"Passenger 2 ID"}%-15s ${"Number Of Flights Together"}%-15s ${"From Date"}%-15s ${"To Date"}%-15s")
      pairsWithCountNTimes.slice(0,20).foreach { case (pax1, pax2, flown, from, to) =>
        println(f"$pax1%-15d $pax2%-15d $flown%-25s $from%-20s $to%-15s")
      }
      println("\n************************* Output 4.1 Ends *************************************************\n")
      writeOutputsToCsv("output-flownTogetherNTimes.csv", pairsWithCountNTimes, outputHdrs )
      pairsWithCountNTimes
    }

    def writeOutputsToCsv(fileName: String, dataSeq: Seq[Any], hdrSeq: Seq[String]): Unit ={

      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
      val dataStringSeq: Seq[String] = dataSeq.map(_.toString.stripSuffix(")").stripPrefix("("))
      writer.write(hdrSeq.mkString(",") + "\n")
      writer.write(dataStringSeq.mkString("\n"))
      writer.close()
    }

  }
  
}
