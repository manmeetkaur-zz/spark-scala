package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

/*
 * Program to count the number of questions in the dataset and collect question ids
 * 
 */
object one {
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val ques = data
                .filter(line => line.trim().startsWith("<row"))
                .filter(line => line.trim().contains("PostTypeId=\"1\""))
    
    val ques_ids = ques.flatMap(line => {
      val xml = XML.loadString(line.trim())
      xml.attribute("Id")
    })
    
    print(ques.count())
    
    ques_ids.foreach(print)
    
    spark.stop()
  }
}