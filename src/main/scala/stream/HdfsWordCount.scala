package stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  *  spark-submit --class "stream.HdfsWordCount" --master local[*] sparkapp_2.11-1.0.jar
  */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hdfsWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(20))

    val lines = ssc.textFileStream("/app/bigdata/log/abc.txt")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x =>(x,1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
