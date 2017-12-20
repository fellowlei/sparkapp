package sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.DataFrameSql" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameSql {

  case class people(id:Int,name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameSql").setMaster("local[*]")
    val sc =new SparkContext(conf);
    val sqlContext =new SQLContext(sc);

    val file = sc.textFile("data/people.txt")
    val log = file.map(line => line.split(",").filter(line => line.length>=3)
      .map(line => people(line(0).toInt,line(1).toString,line(2).toInt)))
    // 方法一、利用隐式转换
    import sqlContext.implicits._
    val peopleDF = log.toDF();
    val df = peopleDF.registerTempTable("people")

    // 写数据到db
    val url = "jdbc:mysql://localhost:3306/spark"
    val table ="people";
    val prop = new java.util.Properties
    prop.setProperty("user","root")
    prop.setProperty("password","1")
    sqlContext.sql("select * from people").write.mode(SaveMode.Append).jdbc(url,table,prop); // 表可以不存在
    sc.stop()
  }
}
