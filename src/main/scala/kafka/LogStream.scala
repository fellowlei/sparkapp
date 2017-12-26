package kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "kafka.LogStream" --master local[*] sparkapp_2.11-1.0.jar
  */
object LogStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogStream").setMaster("local[*]")
    val sc =new SparkContext(conf)
    //每5秒一个批次
    val ssc =new StreamingContext(sc,Seconds(5))

    val topic = Array("test")

    val group = "con-consumer-group"
//    KafkaUtils
    val kafkaParam = Map(
      "bootstrap.servers" ->"172.28.5.2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topic,kafkaParam))
    stream.map(s =>(s.key(),s.value())).print();
    ssc.start();
    ssc.awaitTermination();



  }
}
