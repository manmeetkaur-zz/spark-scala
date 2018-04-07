package org.manmeet.titanic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object all {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Question 2").getOrCreate()
    import spark.implicits._
    val data = spark.read.csv("file:///Users/manmeetkaur/Downloads/titanic_data.txt")
                      .withColumnRenamed("_c0", "row_num")
                      .withColumnRenamed("_c1", "pclass")
                      .withColumnRenamed("_c2", "survived")
                      .withColumnRenamed("_c3", "name")
                      .withColumnRenamed("_c4", "age")
                      .withColumnRenamed("_c5", "embarked")
                      .withColumnRenamed("_c6", "destination")
                      .withColumnRenamed("_c7", "room")
                      .withColumnRenamed("_c8", "ticket")
                      .withColumnRenamed("_c9", "boat")
                      .withColumnRenamed("_c10", "sex")

    data.cache()
    data.count()
    
    
//    one:
    val survived_data = data.filter($"survived" === 1)  // or val survived_data = data.filter("survived == 1")
    
    val died_data = data.filter("survived != 1") 
    
    died_data.groupBy().agg(avg($"age")).show()
    
    survived_data.groupBy().agg(avg($"age")).show()
    
//    two:
    data.createOrReplaceTempView("titanic_data")
    
    val sql2 = """
      select sex, age_group, count(1) from 
      (
      select 
        sex
        , case 
            when age <=20 
              then 'less than 20' 
            when age > 20 and age <= 50 
              then 'more than 20 and less than 50' 
            when (age > 50 or age is null or age = '')
              then 'more than 50 or unknown'
        end as age_group 
      from titanic_data) base
      group by sex, age_group
      order by 1, 2
      """
    spark.sql(sql2).show
    
//    three:
    
    val sql3 = """
      select 
        embarked
        , count(1)
      from titanic_data
      group by embarked
      """
    spark.sql(sql3).show
    
//    four:
    
    val sql4 = """
        select 
          pclass
          , count(1) as survived
        from titanic_data
        where survived = 1
        group by pclass
      """
    spark.sql(sql4).show
    
//    five:
    
    val sql5 = """
        select 
          count(1) as count_of_males
        from titanic_data
        where lower(sex) = 'male'
          and pclass = '2nd'
      """
    spark.sql(sql5).show
  }
}