package com.ng.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author: Cedaris
 * @Date: 2019/6/24 11:02
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<Tuple2<String, String>> dataStream = env.addSource(new HBaseReader());

//        dataStream.map(x => println(x._1 + " " + x._2))
        env.execute();
    }

    public void write2HBaseWithRichSinkFunction() throws Exception {
        String topic = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.187.201:9092");
        props.put("group.id", "kv_flink");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization" +
                ".StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common" +
                ".serialization.StringDeserializer");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> dataStream = null;
        dataStream.addSink(new HBaseWriter());

        env.execute();
    }
}
