package org.manmeet.se

import scala.xml.XML

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
/*
 * Questions which are marked closed for each category – provide the distribution of number of closed questions per month
 */

object eight {
  
  def main (args: Array[String]) {
    
    val orig_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val new_format = new SimpleDateFormat("yyyy-MM-dd")
    
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd
    
    val closed_ques = data
                  .filter(line => (line.trim().startsWith("<row")))
                  .filter(line => line.trim().contains("PostTypeId=\"1\""))
                  .map{line => {
                    val xml = XML.loadString(line.trim())
                    (xml.attribute("ClosedDate"), xml.attribute("Id"))
                  }}
                  .filter(line => line._1 != None)
                  .map{line => {
                    val orig_date = orig_format.parse(line._1.get.text)
                    val new_date = new_format.format(orig_date)
                    (new_date, line._2.get.text)
                    }}
                  .groupByKey

    closed_ques.foreach(println)
    
    spark.stop
  }
  
}