package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameQueryDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameQueryDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameQueryDemo").master("local[*]").getOrCreate();
    // supported by importing this when creating a Dataset.

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val peopleDF = spark.read.json("data/people.json")

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("select name from people where age between 13 and 19")
    teenagerNamesDF.show()




  }
}
