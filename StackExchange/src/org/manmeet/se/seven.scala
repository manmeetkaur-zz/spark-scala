package org.manmeet.se

import scala.xml.XML

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
/*
 * Number of questions which are active for last 6 months
 */

object seven {
  
  def main (args: Array[String]) {
    
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val active_6_months = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .map{line => {
                    val xml = XML.loadString(line)
                    (xml.attribute("CreationDate").get, xml.attribute("LastActivityDate").get, line)
                  }}
                  .filter{line => {
                    val c_date = format.parse(line._1.text)
                    val c_time = c_date.getTime
                    
                    val a_date = format.parse(line._2.text)
                    val a_time = a_date.getTime
                    
                    val time_diff : Long = a_time - c_time
                    
                    time_diff/(1 * 24 * 60 * 60 * 1000) > (6 * 30)
                    }
                  }
    
    active_6_months.foreach(println)
    
    println(active_6_months.count)
    
    spark.stop
  }
  
}