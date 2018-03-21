package example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by lulei on 2018/3/21.
  */
object RawNetworkGrep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RawNetworkGrep")
    // Create the context
    val ssc = new StreamingContext(sparkConf,Duration(1000))


    val numStreams = 10
    val host = "localhost"
    val port = 8888
    val rawSteams = (1 to numStreams).map(_ =>
    ssc.rawSocketStream[String](host,port,StorageLevel.MEMORY_AND_DISK_SER_2)).toArray

    val union = ssc.union(rawSteams)

    union.filter(_.contains("the")).count().foreachRDD(r =>
    println(s"Grep count: ${r.collect().mkString}"))

    ssc.start()
    ssc.awaitTermination()
  }
}
