package demo.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2018/3/14.
  */
object DataFrameTest3 {
  //日志显示级别
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[2]");
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc);

  def readJsonDemo() = {
    val df = sqlContext.read.json("people.json")
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

  def registerTableDemo() = {
    val df = sqlContext.read.json("people.json")
    df.createOrReplaceTempView("people")

    df.show()
    df.printSchema()

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


  def readTxtDemo() = {
    val people = sc.textFile("people.txt")
    val peopleRowRDD = people.map { x => x.split(",") }.map { data => {
      val id = data(0).trim().toInt
      val name = data(1).trim()
      val age = data(2).trim().toInt
      Row(id, name, age)
    }
    }

    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)));

    val df = sqlContext.createDataFrame(peopleRowRDD, structType)

    df.createOrReplaceTempView("people")

    df.show()
    df.printSchema()

  }

  def readTxtDemo2() = {
    case class People(id: Int, name: String, age: Int)

    val people = sc.textFile("people.txt")
    val peopleRDD = people.map { x => x.split(",") }.map { data => {
      People(data(0).trim().toInt, data(1).trim(), data(2).trim().toInt)
    }
    }

    //这里需要隐式转换一把
    import sqlContext.implicits._
    val df = peopleRDD.toDF()
    df.registerTempTable("people")

    df.show()
    df.printSchema()
  }

  def main(args: Array[String]): Unit = {

    //    readJsonDemo();
    //    registerTableDemo()


    //    readTxtDemo()
    readTxtDemo2()

  }
}
