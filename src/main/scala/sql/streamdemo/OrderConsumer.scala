package sql.streamdemo

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by lulei on 2018/2/9.
  */
object OrderConsumer {
  def main(args: Array[String]): Unit = {

    //每件商品总销售额
    val orderTotalKey = "app::order::total"
    //每件商品上一分钟销售额
    val oneMinTotalKey = "app::order::product"
    //总销售额
    val totalKey = "app::order::all"

    // 创建 StreamingContext 时间片为1秒
    val conf = new SparkConf().setMaster("local[*]").setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Kafka 配置
    val kafkaParam = Map(
      "bootstrap.servers" ->"127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroup",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 创建一个 direct stream
    val topic = Array("order")
    var kafkaStream=KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topic,kafkaParam))

    //解析JSON
    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line.value())))
    // 按ID分组统计个数与价格总合
    val orders = events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))

    //输出
    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {
          println("id=" + x._1 + " count=" + x._2 + " price=" + x._3)

          val id = x._1;
          val count = x._2;
          val price = x._3;
          val jedis = new Jedis("localhost",6379);
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1.toString, x._3.toInt)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3.toInt)
        })
    ))
    //输出
    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {

          println("id=" + x._1 + " count=" + x._2 + " price=" + x._3)
          //保存到Redis中
          val jedis = new Jedis("localhost",6379);
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1, x._3)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3)

        })
      ))


    ssc.start()
    ssc.awaitTermination()

  }
}
