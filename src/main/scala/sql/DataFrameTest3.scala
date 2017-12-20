package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.DataFrameTest3" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameTest3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("DataFrameTest3")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("data/people.txt")

    val peopleRDD = people.map{ x=> x.split(",")}.map{data =>{
      val id = data(0).trim.toInt
      val name = data(1).trim
      val age = data(2).trim.toInt
      Row(id,name,age)
    }}

    val structType = StructType(Array(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)))

    val df = sqlContext.createDataFrame(peopleRDD,structType)

    df.registerTempTable("people")

    df.show()
    df.printSchema()
  }
}
