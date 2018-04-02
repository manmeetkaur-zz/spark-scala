package org.manmeet.wc

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCount <input-file> <output-file>") 
      
    }
    
    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val wc = data.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)
    
    wc.saveAsTextFile("out.txt")
    spark.stop
  }
}