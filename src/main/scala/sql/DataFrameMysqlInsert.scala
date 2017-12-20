package sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.DataFrameMysqlInsert" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameMysqlInsert {

  case class People(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameSql").setMaster("local[*]")
    val sc =new SparkContext(conf);
    val sqlContext =new SQLContext(sc);


    val people = sc.textFile("data/people.txt")

    val peopleRDD = people.map { x => x.split(",") }.filter(line => line.length>=3).map(data => {
      People(data(0).trim.toInt, data(1).trim, data(2).trim.toInt)
    })

    //这里需要隐式转换一把
    import sqlContext.implicits._
    val df = peopleRDD.toDF()
    df.registerTempTable("people")
    sqlContext.sql("select * from people").show()

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
