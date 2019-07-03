package com.ng.flink.DataLoader;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * @Author: Cedaris
 * @Date: 2019/7/3 13:58
 */
public class KafkaProducer {
    public static void main(String[] args) {
        String topic = "bookTopN";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers","psy831:9092");
        FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<>(
                topic, new SimpleStringSchema(), props
        );

        producer.setWriteTimestampToKafka(true);
        text.addSink(producer);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
