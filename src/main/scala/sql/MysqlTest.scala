package sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.MysqlTest" --master local[*] sparkapp_2.11-1.0.jar
  */
object MysqlTest {

  //数据库配置
  lazy val url = "jdbc:mysql://localhost:3306/spark"
  lazy val username = "root"
  lazy val password = "1"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")
    //序列化
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryoserializer.buffer", "256m")
//    conf.set("spark.kryoserializer.buffer.max", "2046m")
//    conf.set("spark.akka.frameSize", "500")
//    conf.set("spark.rpc.askTimeout", "30")

    //获取context
    val sc = new SparkContext(conf)

    //获取sqlContext
    val sqlContext = new SQLContext(sc)

    //引入隐式转换，可以使用spark sql内置函数
    import sqlContext.implicits._


    //创建jdbc连接信息
    val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"
    val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "com.mysql.jdbc.Driver")
    //加载mysql数据表
    val df: DataFrame = sqlContext.read.jdbc(uri, "people", prop)


    //从dataframe中获取所需字段
    df.select("id", "name", "age").collect()
      .foreach(row => {
        println("id  " + row(0) + " ,name  " + row(1) + ", age  " + row(2))
      })
    //注册成临时表
    df.registerTempTable("temp_table")

    val total_sql = "select * from temp_table "
    val total_df: DataFrame = sqlContext.sql(total_sql)

    //将结果写入数据库中
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","1")
    total_df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark?useUnicode=true&characterEncoding=UTF-8","people",properties)
    //分组后求平均值
    total_df.groupBy("name").avg("age").collect().foreach(x => {
      println("name " + x(0) + ",age " + x(1))
    })



  }
}
