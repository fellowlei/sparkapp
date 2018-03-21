package example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lulei on 2018/3/21.
  */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {

    // Create the context
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val path = "d:/log.txt"
    val lines = ssc.textFileStream(path)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x =>(x,1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
