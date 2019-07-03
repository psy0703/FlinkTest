package com.ng.flink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
/**
 * flink实时处理kafka传来的数据通过连接池技术写入hbase
 * From Duhanmin
 * @Author: Cedaris
 * @Date: 2019/6/20 15:06
 */
public class flink2hbase {
    public static void main(String[] args) {
        /*try {
            writeToKafka();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        try {
            readKafka();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static final String broker_list = "psy831:9092,psy832:9092," +
            "psy833:9092";
    //kafka topic 需要和 flink 程序用同一个 topic
    public static final String topic = "headline";
    public static final String groupid = "test02";
    public static final String zookeepers = "psy831:2181,psy832:2181," +
            "psy833:2181";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String mes = "421032" + i + ",,,,55825,2,,42,4202,420202," +
                    "420202403," +
                    "湖北省,黄石市," +
                    "黄石港区, 黄石港区黄石港片区工作委员会,,,,,,,,,,,青春不再回头,,," +
                    "2019-06-0322:45:12,半壶纱,刘珂矣,http://jxtt.diangoumall.com/149c30b0vodcq1258058953/6fbc2dc55285890789825841788/5285890789825841789.jpg,,,2019-06-0323:59:00,,,,,,,,,,,\n";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, mes);
            producer.send(record);
            Thread.sleep(10 * 1000);
        }
        producer.flush();
    }

    public static void readKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        Properties props = new Properties();
        props.put("bootstrap.servers",broker_list);
        props.put("zookeeper.connect",zookeepers);
        props.put("group.id", groupid);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        DataStreamSource<String> data = env.addSource(new FlinkKafkaConsumer011<String>(
                topic,
                new SimpleStringSchema(),
                props
        ));

        SingleOutputStreamOperator<String[]> item = data.setParallelism(1).map(string -> {
            String[] words = string.split(",", 46);
            return words;
        });

        //设置窗口时间为两秒
        item.timeWindowAll(Time.seconds(2)).apply(new AllWindowFunction<String[],
                String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow,
                              Iterable<String[]> value,
                              Collector<String> out) throws Exception {
                for (String[] strings : value) {
                    String userid = strings[0];
                    System.out.println("user_id --> " + userid);
                }

            }
        });

        env.execute("kafkadata");

    }
}
