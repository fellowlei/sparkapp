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
    val redis = new Jedis("localhost",6379);
    val keys = Array("app::order::total","app::order::product","app::order::all");
    for(key <- keys){
      redis.set(key,key);
    }

    for(key <- keys){
      println(redis.get(key))
    }
  }
}
