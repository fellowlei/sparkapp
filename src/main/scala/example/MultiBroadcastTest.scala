package example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/20.
  */
object MultiBroadcastTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MultiBroadcastTest").master("local[*]").getOrCreate()

    val slices = 2
    val num = 100000

    val arr1 = new Array[Int](num)
    for(i <- 0 until  arr1.length){
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for(i <- 0 until arr2.length){
      arr2(i) = i
    }

    val bar1 = spark.sparkContext.broadcast(arr1)
    val bar2 = spark.sparkContext.broadcast(arr2)

    val observedSize:RDD[(Int,Int)] = spark.sparkContext.parallelize(1 to 10,slices).map { _ =>
      (bar1.value.length, bar2.value.length)
    }
   observedSize.collect().foreach(i => println(i))

    spark.stop()


  }
}
