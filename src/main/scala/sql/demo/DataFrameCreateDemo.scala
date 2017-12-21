package sql.demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameCreateDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameCreateDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCreateDemo").master("local[*]").getOrCreate();
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("data/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    results.show()


  }
}
