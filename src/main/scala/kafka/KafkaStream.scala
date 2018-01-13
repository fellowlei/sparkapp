package kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "kafka.KafkaStream" --master local[*] sparkapp_2.11-1.0.jar
  */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaStream").setMaster("local[*]")
    val sc =new SparkContext(conf)
    val ssc =new StreamingContext(sc,Seconds(2)) // 每5秒一个批次


    val group = "con-consumer-group"
    val kafkaIp ="127.0.0.1:9092"
//    val kafkaIp ="172.28.5.2:9092"

//    KafkaUtils
    val kafkaParam = Map(
      "bootstrap.servers" ->kafkaIp,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //创建DStream，返回接收到的输入数据
    val topic = Array("ip_collect")
    var stream=KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topic,kafkaParam))
//    stream.map(s =>(s.key(),s.value())).print();

    val wordCounts = stream.map(x =>(x.value(),1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start();
    ssc.awaitTermination();



  }
}
