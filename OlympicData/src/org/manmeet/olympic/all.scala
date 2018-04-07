package org.manmeet.olympic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object all {
  def main (args: Array[String]) {
  
    val spark = SparkSession.builder.appName("Olympic Data").getOrCreate()
    
    val data = spark.read.csv("file:///Users/manmeetkaur/Downloads/olympic_Data.csv")
                  .withColumnRenamed("_c0", "name")
                  .withColumnRenamed("_c1", "age")
                  .withColumnRenamed("_c2", "country")
                  .withColumnRenamed("_c3", "year")
                  .withColumnRenamed("_c4", "closing_date")
                  .withColumnRenamed("_c5", "sport")
                  .withColumnRenamed("_c6", "gold")
                  .withColumnRenamed("_c7", "silver")
                  .withColumnRenamed("_c8", "bronze")
                  .withColumnRenamed("_c9", "total")
    
    data.cache
    data.count()
    data.createOrReplaceTempView("olympic_data")
    
  //  Ans one:
    
    val sql1 = """
      select 
        year
        , count(distinct name)
      from olympic_data
      group by year
      """
    
    spark.sql(sql1).show
    
//    Ans two:

   val sql2 = """
      select
        year 
        , country
        , count(total) as medal_count
      from olympic_data
      group by country, year
      order by 2, 3 desc
      """
    
    spark.sql(sql2).show
    
//    Ans three:

   val sql3 = """
      select 
        name
        , count(gold) as gold_medals
      from olympic_data
      group by name
      order by 2 desc
      """
    
    spark.sql(sql3).show(numRows=10)
    
//    Ans four:

   val sql4 = """
      select 
        count(distinct name) as athlete_count
      from olympic_data
      where age < 20
        and gold > 0 and gold is not null
      """
    
    spark.sql(sql4).show    

//    Ans five:

   val sql5 = """
     select name, age, sport, year
     from 
     ( 
      select
        name
        , year
        , sport
        , age
        , row_number() over (partition by year, sport order by age) as row_num
      from olympic_data
      where gold > 0 and gold is not null
      ) base where row_num = 1
      """
    
    spark.sql(sql5).show
    
//    Ans six:

   val sql6 = """
     select name, age, sport, year
     from 
     ( 
      select
        name
        , year
        , sport
        , age
        , row_number() over (partition by year, sport order by age) as row_num
      from olympic_data
      where gold > 0 and gold is not null
      ) base where row_num = 1
      """
    
    spark.sql(sql6).show    
  }
}