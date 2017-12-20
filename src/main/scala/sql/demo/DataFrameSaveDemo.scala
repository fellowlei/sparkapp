package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameSaveDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameSaveDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameSaveDemo").master("local[*]").getOrCreate();

    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("data/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("select name from parquetfile where age between 13 and 19")
    namesDF.map(attributes => "name: " + attributes(0)).show()
  }
}
