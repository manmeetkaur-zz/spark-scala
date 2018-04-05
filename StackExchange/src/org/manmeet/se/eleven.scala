package org.manmeet.se

import scala.xml.XML
import java.sql.Date
//import java.time.LocalDate
//import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
/*
 * Number of question with specific tags (nosql, big data) which was asked in the specified time range (from 01-01-2015 to 31-12-2015)
 */

object eleven {
  
  def main (args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val custom_format = new SimpleDateFormat("yyyy-MM-dd")
    
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val select_ques = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .map{line => {
                    val xml = XML.loadString(line)
                    (xml.attribute("Tags").getOrElse("").toString.toLowerCase, xml.attribute("CreationDate").get.toString)
                  }}
                  .filter{line => { 
                    val start_date = custom_format.parse("2015-01-01").getTime
                    
                    val end_date = custom_format.parse("2015-12-31").getTime
                    
                    val creation_date = format.parse(line._2).getTime
                    
                    line._1.matches(".*(big data|nosql).*") && (creation_date >= start_date) && (creation_date <= end_date)
                  }}
                  
//LocalDate is not recognized by Spark Scala Shell due to java version conflict, so doing the seconds from epoch way
//                  .filter{line => {
//                    val start_date = LocalDate.of(2015,1,1)
//                    val end_date = LocalDate.of(2015,12,31)
//                    line._1.matches(".*(big data|nosql).*") && ((LocalDate.parse(line._2, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")).isAfter(start_date)) && (LocalDate.parse(line._2, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")).isBefore(end_date)))
//                  }}

    print(select_ques.count)
    
    spark.stop
  }
  
}