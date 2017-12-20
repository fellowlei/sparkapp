package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameCreateDemo2" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameCreateDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCreateDemo2").master("local[*]").getOrCreate();

    val usersDF = spark.read.load("data/users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json").load("data/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    // Run SQL on files directly
    val sqlDF = spark.sql("SELECT * FROM parquet.`data/users.parquet`")


    // Bucketing, Sorting and Partitioning
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    peopleDF.write.partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("people_partitioned_bucketed")




  }
}
