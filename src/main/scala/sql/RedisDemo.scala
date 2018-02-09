package sql

import redis.clients.jedis.{Jedis, Response}

/**
  * Created by lulei on 2018/2/9.
  */
object RedisDemo {
  def main(args: Array[String]): Unit = {
//      demo1()
//      demo2()
//      demo3()
    demo4()
  }

  def demo1(): Unit ={
    val redis = new Jedis("localhost",6379);
    redis.set("name","mark");
    val name = redis.get("name");
    println(name)
  }

  /**
    * multi key
    */
  def demo2(): Unit ={
    val redis = new Jedis("localhost",6379);
    val keys = Array("name1","name2","name3");
    for(key <- keys){
      redis.set(key,key);
    }

    for(key <- keys){
      println(redis.get(key))
    }
  }

  def demo3(): Unit ={
    val redis = new Jedis("localhost",6379);
    val keys = Array("name1","name2","name3");
    val pp = redis.pipelined();

    var redisRes = Map[String,Response[String]]();
    for(key <- keys){
      redisRes ++= Map(key -> pp.get(key))
    }
    pp.sync()
    println(redisRes)
  }

  def demo4(): Unit ={
    val redis = new Jedis("localhost",6379);
    val keys = Array("name1","name2","name3");
    var tryTimes = 2
    var flag = false
    var redisResp = Map[String,Response[String]]();
    while(tryTimes > 0 && !flag){
      try{
        val pp = redis.pipelined()
        for(key <- keys){
          redisResp ++= Map(key -> pp.get(key))
        }
        pp.sync()
        flag = true
      }catch {
        case e: Exception => {
          flag = false
          println("Redis-Timeout" + e)
          tryTimes = tryTimes - 1
        }
      }finally{
        redis.disconnect()
      }
    }

  }
}
