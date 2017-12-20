package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.DataFrameTest2" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameTest2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("DataFrameTest2")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("people.json")

    df.registerTempTable("people")

    df.show();
    df.printSchema();

    //查看某个字段
    sqlContext.sql("select name from people ").show()
    //查看多个字段
    sqlContext.sql("select name,age+1 from people ").show()
    //过滤某个字段的值
    sqlContext.sql("select age from people where age>=25").show()
    //count group 某个字段的值
    sqlContext.sql("select age,count(*) cnt from people group by age").show()

    //foreach 处理各字段返回值
    sqlContext.sql("select id,name,age  from people ").foreach { x => {
      //通过下标获取数据
      println("col1: " + x.get(0) + ", col2: " + "name: " + x.get(2) + ", col3: " + x.get(2))
    }
    }

    //foreachPartition 处理各字段返回值，生产中常用的方式
    sqlContext.sql("select id,name,age  from people ").foreachPartition { iterator =>
      iterator.foreach(x => {
        //通过字段名获取数据
        println("id: " + x.getAs("id") + ", age: " + "name: " + x.getAs("name") + ", age: " + x.getAs("age"))

      })
    }
  }
}
