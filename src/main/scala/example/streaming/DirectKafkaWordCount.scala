package example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lulei on 2018/3/21.
  */
object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    // Create direct kafka stream with brokers and topics
    val brokers  = "localhost:2181"
    val topics = "mytopic1,mytopic2"
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers);

    val messages = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines =messages.map(_.value())
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x =>(x,1)).reduceByKey(_ + _)

    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
