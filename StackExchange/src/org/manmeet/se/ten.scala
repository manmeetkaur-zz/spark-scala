package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession
/*
 * List all of the tags and their counts
 */

object ten {
  
  def main (args: Array[String]) {
    
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val flatten_tags = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .map{line => {
                    val xml = XML.loadString(line)
                    xml.attribute("Tags").get.toString
                  }}
                  .flatMap(line => line.replaceAll("&lt;", ",").replaceAll("&gt;", ",").split(","))
                  .filter(line => line != "")
                  .map(line => (line, 1))
                  .reduceByKey(_ + _)


    flatten_tags.take(10).foreach(println)
    
    spark.stop
  }
  
}