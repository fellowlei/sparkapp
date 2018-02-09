package sql.streamdemo

import redis.clients.jedis.Jedis

/**
  * Created by lulei on 2018/2/9.
  */
object RedisUtil {
  def main(args: Array[String]): Unit = {
    queryOrder();
  }

  def queryOrder(): Unit ={
    val total ="app::order::total";
    val produce ="app::order::product"
    val all = "app::order::all";
    val redis = new Jedis("localhost",6379);
    val keys = Array(total,produce,all);
    for(key <- keys){
      redis.set(key,key);
    }

    for(key <- keys){
      println(redis.get(key))
    }

    redis.hincrBy("order::total","aa",1L);
    println(redis.get("order::total"))
  }
}
