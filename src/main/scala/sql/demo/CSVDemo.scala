package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/21.
  * spark-submit --class "sql.demo.CSVDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object CSVDemo {
  def main(args: Array[String]): Unit = {
    // sparkContext
    val sparkSession = SparkSession.builder.master("local[*]")
      .appName("CSVDemo")
      .getOrCreate()

    val df = sparkSession.read.option("header","true").
      csv("data/student.csv")
    df.show()

  }
}
