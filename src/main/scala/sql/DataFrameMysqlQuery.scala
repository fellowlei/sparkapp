package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit   --class "sql.DataFrameMysqlQuery" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameMysqlQuery {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("MysqlDataFrameTest").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val url = "jdbc:mysql://localhost:3306/spark"
    val table ="people";
    val prop = new java.util.Properties
    prop.setProperty("user","root")
    prop.setProperty("password","1")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    val jdbcDF = sqlContext.read.jdbc(url,table,prop)
    jdbcDF.registerTempTable("people")

    sqlContext.sql("select * from people").show()



  }

}
