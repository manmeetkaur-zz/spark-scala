package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession
/*
 * Number of questions with more than 2 answers
 */

object six {
  
  def main (args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val gt2_answer = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .filter{line => {
                    val xml = XML.loadString(line)
                    Integer.parseInt(xml.attribute("AnswerCount").getOrElse(0).toString) > 2
                  }}
    
    gt2_answer.foreach(println)
    
    println(gt2_answer.count)
    
    spark.stop
  }
  
}