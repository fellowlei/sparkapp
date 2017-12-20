package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.DataFrameTest4" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameTest4 {

  case class People(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("DataFrameTest4").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("data/people.txt")

    val peopleRDD = people.map { x => x.split(",") }.map(data => {
      People(data(0).trim.toInt, data(1).trim, data(2).trim.toInt)
    })

    //这里需要隐式转换一把
    import sqlContext.implicits._
    val df = peopleRDD.toDF()
    df.registerTempTable("people")

    df.show()
    df.printSchema()
  }
}
