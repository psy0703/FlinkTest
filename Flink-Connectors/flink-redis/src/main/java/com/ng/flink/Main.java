package com.ng.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

/**
 * @Author: Cedaris
 * @Date: 2019/6/25 10:13
 */
public class Main {
    public static void main(String[] args) {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("217.0.0.0").build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> word = env.fromElements("Hello World");
//        word.addSink(new RedisSink<Tuple2<String, String>>(conf,new RedisExampleMapper()));


        //Redis Cluster
        /*FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(Arrays.asList(new InetSocketAddress(5601)))).build();

        DataStream<String> stream = ...;
        stream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper());*/
    }
}
