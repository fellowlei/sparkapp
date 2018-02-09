package sql

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2018/2/9.
  */
object JDBCDemo {


  def main(args: Array[String]): Unit = {
//    demo1()
//    demo2()
//    demo3()
//    demo4()
//    demo5()
//    demo6()
    demo7()
  }

  def demo1(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test?user=root&password=1"
    val prop = new Properties()
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.jdbc(url,"user",prop);
    println(df.count())
    println(df.rdd.partitions.size)
  }

  def demo2(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test?user=root&password=1"
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 3
    val prop = new Properties()

    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.jdbc(url,"user","id",lowerBound,upperBound,numPartitions,prop);
    println(df.count())
    println(df.rdd.partitions.size)

  }

  def demo3(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test?user=root&password=1"
    val predicates = Array[String]("id < 2","id >= 2")
    val prop = new Properties()
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.jdbc(url,"user",predicates,prop);
    df.show()
    println(df.count())
    println(df.rdd.partitions.size)
  }

  def demo4(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test?user=root&password=1"
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("jdbc").options(Map("url"->url,"dbtable" -> "user")).load();
    df.show()
    println(df.count())
    println(df.rdd.partitions.size)
  }

  def demo5(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test"
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.addJar("D:\\workspace\\sparkApp\\lib\\mysql-connector-java-5.0.8-bin.jar")
    val sqlContext = new SQLContext(sc)

    // read
    val df = sqlContext.read.format("jdbc").options(Map("url"->url,"user"->"root","password"->"1","dbtable"->"user")).load()
    df.show();
    df.collect().take(3).foreach(println)
//    df.rdd.saveAsTextFile("user_out");


    // write
   val url2 = "jdbc:mysql://localhost:3306/test"
    val prop = new Properties();
    prop.setProperty("user","root");
    prop.setProperty("password","1");
    df.write.mode(SaveMode.Overwrite).jdbc(url,"user2",prop);
//    df.write.mode(SaveMode.Append).jdbc(url,"user2",prop);



  }

  def myFun(iterator: Iterator[(Int,String,Int)]):Unit={
    val url = "jdbc:mysql://localhost:3306/test"
    val sql = "insert into user(id,name,age) values(?,?,?)"
    val conn = DriverManager.getConnection(url,"root","1");
    val pstmt = conn.prepareStatement(sql);
    iterator.foreach(data =>{
      pstmt.setInt(1,data._1)
      pstmt.setString(2,data._2)
      pstmt.setInt(3,data._3)
      val result = pstmt.executeUpdate()
      println(result)
    })
  }

  /**
    * insert demo
    */
  def demo6(): Unit ={
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val list=List((2,"mark2",2),(3,"mark3",3),(4,"mark4",4));
    val data:RDD[(Int,String,Int)] = sc.parallelize(list);
    data.foreachPartition(myFun);
    sc.stop();

  }

  def demo7(): Unit ={
    val url = "jdbc:mysql://localhost:3306/test"
    val conf = new SparkConf().setAppName("JDBCDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val personRDD = sc.parallelize(Array("5 tom 5", "6 jerry 6", "7 kitty 7")).map(_.split(" "))
    val schema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )

    val rowRDD  = personRDD.map(p =>Row(p(0).toInt,p(1).trim,p(2).toInt))

    val df = sqlContext.createDataFrame(rowRDD,schema);
    df.show()

    val prop = new Properties();
    prop.put("user","root")
    prop.put("password","1")
    df.write.mode(SaveMode.Append).jdbc(url,"user",prop);
    sc.stop()
  }
}


