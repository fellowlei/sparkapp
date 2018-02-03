package redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool}

object RedisUtil {

  def getJedisPool():JedisPool = {
    val jedisPool = new JedisPool(new GenericObjectPoolConfig(), "localhost", 6379, 30000)
    jedisPool;
  }

  def set(key:String,myval:String) = {
    val jedisPool = getJedisPool();
    val jedis = jedisPool.getResource;
    jedis.set(key,myval);
//    val value = jedis.get("name");
    println(myval)
  }

  def main(args: Array[String]): Unit = {
    val jedisPool = getJedisPool();
    val jedis = jedisPool.getResource;
//    jedis.set("name","mark");
    val value = jedis.get("name");
    println(value)


  }
}
