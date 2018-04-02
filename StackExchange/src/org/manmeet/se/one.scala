package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

object one {
  
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: two <input-file> <output-file>")
//      System.exit(1)
//    }
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeet/Downloads/Posts.xml").rdd
    
    val ques = data
                .filter(line => line.trim().startsWith("<row"))
                .filter(line => line.trim().contains("PostTypeId=\"1\""))
    
    val ques_ids = ques.map(line => {
      val xml = XML.loadString(line)
      xml.attribute("PostTypeId")
    }
    
    print(ques.count())
    
    spark.stop()
  }
}