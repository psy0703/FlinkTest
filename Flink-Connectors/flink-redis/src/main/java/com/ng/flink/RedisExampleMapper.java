package com.ng.flink;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.Tuple2;

/**
 * how to create a sink that communicate to a single redis server
 * @Author: Cedaris
 * @Date: 2019/6/25 10:10
 */
public class RedisExampleMapper implements RedisMapper<Tuple2<String,String>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data._1;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data._2;
    }
}
