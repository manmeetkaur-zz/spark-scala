package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

/*
 * The trending questions which are viewed and scored highly by the user – Top 10 highest viewed questions with specific tags
 * 1. filter the questions that do not have any tags
 * 2. sort by View, descending
 * 3. and then sort by score descending
 * 4. print top 10
 */

object four {
  
  def main(args: Array[String]) {
      
      val spark = SparkSession.builder().appName("Question 2").getOrCreate()
      val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
      
      val view_sorted = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .filter{line => {
                    val xml = XML.loadString(line)
                    xml.attribute("Tags") != ""
                  }}
                  .map{line => {
                    val xml = XML.loadString(line)
                    (Integer.parseInt(xml.attribute("ViewCount").getOrElse(0).toString), (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString), line))
                  }}
                  .sortByKey(false)
                  
      val score_sorted = view_sorted
                          .map{case (k,v) => v}
                          .sortByKey(false)

      score_sorted.take(10).foreach(println)
      
      spark.stop()
  }  
}