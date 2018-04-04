package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

/*
 * Provide the number of posts which are questions and contains specified words in their title 
 * (like data, science, nosql, hadoop, spark)
 */

object three {
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val ques = data
                .filter(line => (line.trim().startsWith("<row")))
                .filter(line => line.trim().contains("PostTypeId=\"1\""))
                .flatMap(line => {
                  val xml = XML.loadString(line)
                  xml.attribute("Title")
                })
//                .map{
//                  case (line) =>  
//                    val strRegex = ".*(hadoop|spark|science|data).*".r
//                    val ifMatch = strRegex.findFirstIn(line.toString)
//                    (line, ifMatch)
//                }
                .filter(line => line.toString.matches(".*(hadoop|spark|science|data).*"))
    
    println(ques.count())
    
    spark.stop()
  }
}