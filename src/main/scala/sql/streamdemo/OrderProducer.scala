package sql.streamdemo

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Created by lulei on 2018/2/9.
  */
object OrderProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties();
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
//    props.put("retries", 1)
//    props.put("batch.size", 16384)
//    props.put("linger.ms", 1)
//    props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //创建生产者
    val producer = new KafkaProducer[String, String](props)

    while(true){
      //随机生成10以内ID
      val id = Random.nextInt(10)
      //创建订单成交事件
      val event =new JSONObject();
      //商品ID
      event.put("id", id)
      //商品成交价格
      event.put("price", Random.nextInt(10000))
      producer.send(new ProducerRecord[String, String]("order", "", event.toString))
    }
  }
}
