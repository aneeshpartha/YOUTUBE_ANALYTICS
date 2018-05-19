/**
  * Created by Aneesh Partha on 2/10/2018.
  */

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.io._
import scala.collection.immutable

object  youtubeanalytics extends App {

  try {
    def filter(data: Iterator[String]) = {

      // Filtering out heading
      val filter1 = data.drop(1)

      // Few records starts with \n. Filtering out the data
      val filter2 = filter1.filter(rec => rec.startsWith("\\n") == false)

      // Few records starts with http which is a bad record for our analysis. Filtering the data
      val filter3 = filter2.filter(rec => rec.startsWith("http") == false)

      // Processing data from csv file. Hence the delimiter must be comma. If the comma is not available then it must be the continuation of previous line
      val filter4 = filter3.filter(rec => rec.contains(",") == true)

      // The first field must be video id which must be of 11 characters in length
      val filter6 = filter4.filter(rec => rec.split(",")(0).length == 11)

      // Our records has 16 fields hence the number of delimiters must be 15
      val filter7 = filter6.filter(rec => (rec.count(_ == ',') == 15))

      // returning data
      filter7
    }


    def process(data: Iterator[String]) = {

      // Splitting the data as a tuple
      val process1 = data.map(rec => {

        val d = rec.split(",")
        (d(1), d(7).toInt, d(0), d(2), d(3), d(8), d(9))
      })


      // Converting data to sequence and using the sort API to sort data based on date and views in descending order
      val process2 = process1.toSeq.sortBy((rec => (rec._1, rec._2))).reverse

      // First record for each date is fetched
      val process3 = process2.groupBy(rec => (rec._1))

      // returning the processed data
      process3


    }


    // CSV file is read. Different CSV files can be given as argument

    val srcfile = Source.fromFile(args(0))("UTF-8").getLines()

    // Calling filter function to filter out unwanted data
    val filtereddata = filter(srcfile)


    // function call to process data.
    val processeddata = process(filtereddata)

    // Heading for the output
    println("Date" + "\t" + "No of views" + "\t" + "Video_id" + "\t" + "channel" + "\t\t\t" + "Description" + "\t" + "likes")

    // Output is formatted to make it readable
    val data = processeddata.map(rec => rec._2(0)).map(rec => (rec._1 + "\t" + rec._2 + "\t" + rec._3 +
      "\t" + rec._4 + "\t\t\t" + rec._5 + "\t" + rec._6 + "\t" + rec._7))

    // Printing output to the console
    data.foreach(println)
  }
  catch
  {
    case num:NumberFormatException => print("There is a number format exception")
  }
}

