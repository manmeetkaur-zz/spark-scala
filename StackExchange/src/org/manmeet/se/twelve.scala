package org.manmeet.se

import scala.xml.XML

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.lang.String
import java.util.Date
import org.apache.spark.sql.SparkSession

object twelve {
	def main(args: Array[String]) = {
			
	    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");

			val spark = SparkSession.builder().appName("Question 2").getOrCreate()
				
      val data = spark.read.textFile("file:///Users/manmeetkaur/Downloads/Posts.xml").rdd

			val baseData = data
			             .filter{line => 
			                  {line.trim().startsWith("<row")}
			                }
              			.map {line => {
              			  val xml = XML.loadString(line)
              			  var aaId = "";
              			  if (xml.attribute("AcceptedAnswerId") != None)
              			  {
              			    aaId = xml.attribute("AcceptedAnswerId").get.toString()
              			  }
              			  val crDate = xml.attribute("CreationDate").get.toString()
              			  val rId = xml.attribute("Id").get.toString()
              			  (rId, aaId, crDate)
              			  }
              			}
			
			val aaData = baseData
			            .map{ data => {
			              (data._2, data._3)
			            }}
			            .filter{ data => {data._1.length() > 0}}
			
			val rdata = baseData
			            .map{ data => {
			              (data._1, data._3)
			            }}
			
			val joinData = rdata
			                .join(aaData)
			                .map{ data => {
			                  val quesDate = format.parse(data._2._2).getTime
			                  val ansDate = format.parse(data._2._1).getTime
			                  val diff : Float = ansDate - quesDate
			                  val time : Float = diff/(1000 * 60 * 60)
			                  time
			                }}
			
			val count = joinData.count()
			val result = joinData.sum() / count

			println(result)
			
			spark.stop
	}
}