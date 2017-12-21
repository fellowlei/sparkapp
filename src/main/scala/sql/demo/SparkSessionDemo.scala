package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/21.
  * spark-submit --class "sql.test.SparkSessionDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object SparkSessionDemo {
  def main(args: Array[String]): Unit = {

    // sparkContext
    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()

    // hiveContext
    val sparkSession2 = SparkSession.builder.master("local")
      .appName("spark session example")
      .enableHiveSupport()
      .getOrCreate()

    val df = sparkSession.read.option("header","true").
      csv("data/sales.csv")
    df.show()
  }
}
