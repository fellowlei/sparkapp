package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameSqlDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameSqlDemo {
  case class Person(name:String,age:Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameSqlDemo").master("local[*]").getOrCreate();
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("data/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("select name, age from people where age between 13 and 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "name: " + teenager(0)).show()

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()




  }
}
