package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession
/*
 * The questions that doesn’t have any answers – Number of questions with “0” number of answers
 */

object five {
  
  def main (args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val no_answer = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .filter{line => {
                    val xml = XML.loadString(line)
                    Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString) == 0
                  }}
    
    no_answer.foreach(println)
    
    println(no_answer.count)
    
    spark.stop
  }
  
}