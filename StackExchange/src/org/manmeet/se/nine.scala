package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession
/*
 * The most scored questions with specific tags – Top 10 questions having tag hadoop, spark
 */

object nine {
  
  def main (args: Array[String]) {
    
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val top_scored = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .map{line => {
                    val xml = XML.loadString(line)
                    (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString), xml.attribute("Tags").get)
                  }}
                  .filter(line => line._2 != None && line._2 != "" && line._2.toString.matches(".*(hadoop|spark).*"))
                  .sortByKey(false)

    top_scored.take(10).foreach(println)
    
    spark.stop
  }
  
}