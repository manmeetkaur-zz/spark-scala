package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

/*
 * Monthly questions count –provide the distribution of number of questions asked per month
 * 
 */

object two {
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val ques = data
                .filter(line => (line.trim().startsWith("<row")))
                .filter(line => line.trim().contains("PostTypeId=\"1\""))
                .flatMap(line => {
                  val xml = XML.loadString(line)
                  xml.attribute("CreationDate")
                })
    
    val result = ques.map(line => (line.toString.substring(0,7), 1)).reduceByKey(_ + _)
    
    result.foreach(println)
    
    spark.stop()
  }
}