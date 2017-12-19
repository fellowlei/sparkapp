package stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "stream.NetworkWordCount" --master local[*] gitcodesbt_2.11-1.0.jar 127.0.0.1 9999
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))//5秒间隔

    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK)// 服务器地址，端口，序列化方案

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x =>(x,1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
