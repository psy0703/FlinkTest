package com.ng.flink.Sinks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: Cedaris
 * @Date: 2019/7/3 15:49
 */
public class RedisSinkExample implements RedisMapper<Tuple2<String,Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET,null);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
        return data.f1.toString();
    }
}
