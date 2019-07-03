package com.ng.flink

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisPoolConfig, FlinkJedisSentinelConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @Author: Cedaris
  * @Date: 2019/6/25 10:24
  */
class RedisExampleMapper1 extends RedisMapper[(String,String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"HASH_NAME")
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }

  def main(args: Array[String]): Unit = {
    //single redis server:
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("217.0.0.0").build()

//    stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))

//for Redis Cluster
  /*val conf = new FlinkJedisSentinelConfig.Builder().setMasterName("master")
   .setSentinels(...).build()
    stream.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))*/
  }
}
