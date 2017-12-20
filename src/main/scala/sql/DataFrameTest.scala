package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "sql.DataFrameTest" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    //日志显示级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

    //初始化
    val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("data/people.json")

    //查看df中的数据
    df.show()
    //查看Schema
    df.printSchema()
    //查看某个字段
    df.select("name").show()
    //查看多个字段，plus为加上某值
    df.select(df.col("name"), df.col("age").plus(1)).show()
    //过滤某个字段的值
    df.filter(df.col("age").gt(25)).show()
    //count group 某个字段的值
    df.groupBy("age").count().show()

    //foreach 处理各字段返回值
    df.select(df.col("id"), df.col("name"), df.col("age")).foreach { x => {
      //通过下标获取数据
      println("col1: " + x.get(0) + ", col2: " + "name: " + x.get(2) + ", col3: " + x.get(2))
    }
    }

    //foreachPartition 处理各字段返回值，生产中常用的方式
    df.select(df.col("id"), df.col("name"), df.col("age")).foreachPartition { iterator =>
      iterator.foreach(x => {
        //通过字段名获取数据
        println("id: " + x.getAs("id") + ", age: " + "name: " + x.getAs("name") + ", age: " + x.getAs("age"))

      })
    }

  }
}
