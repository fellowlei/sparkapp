package kafka

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  */
object LogStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogStream").setMaster("local[*]")
    val sc =new SparkContext(conf)
    //每60秒一个批次
    val ssc =new StreamingContext(sc,Seconds(60))

  }
}
