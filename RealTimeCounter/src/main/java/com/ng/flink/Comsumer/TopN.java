package com.ng.flink.Comsumer;

import com.ng.flink.Sinks.MysqlSink;
import com.ng.flink.Sinks.RedisSinkExample;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * 每隔5秒钟 计算过去1小时 的 Top 3 商品
 *
 * @Author: Cedaris
 * @Date: 2019/7/3 14:15
 */
public class TopN {
    public static void main(String[] args) {
        /**
         * 书1 书2 书3
         *（书1,1） (书2，1) （书3,1）
         */
        String topic = "bookTopN";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //以processingTime作为时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO:从kafka获取数据
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "psy831:9092");
        FlinkKafkaConsumer011<String> consumer010 = new FlinkKafkaConsumer011<>(
                topic, new SimpleStringSchema(), props
        );
        consumer010.setStartFromEarliest();

        DataStreamSource<String> stream = env.addSource(consumer010);
        //将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>类型
        DataStream<Tuple2<String, Integer>> ds = stream.flatMap(new LineSpliter());

        DataStream<Tuple2<String, Integer>> wordCount = ds
                .keyBy(0)
                //key之后的元素进入一个总时间长度为600s,每5s向后滑动一次的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600),
                        Time.seconds(5)))
                // 将相同的key的元素第二个count值相加
                .sum(1);

        wordCount
                //所有key元素进入一个5s长的窗口（选5秒是因为上游窗口每5s计算一轮数据，
                //topN窗口一次计算只统计一个窗口时间内的变化）
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopNAllFunction(3))
                .print();

        //TODO:将数据写入Redis
        /*//实例化FlinkJedisPoolConfig 配置redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        wordCount.addSink(new RedisSink<>(jedisPoolConfig,
                new RedisSinkExample()));*/

        //TODO:将数据写入MySQL
        wordCount.addSink(new MysqlSink());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class LineSpliter implements FlatMapFunction<String,
            Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            Tuple2<String, Integer> tuple2 = new Tuple2<>(value, 1);
            out.collect(tuple2);
        }
    }

    private static class TopNAllFunction
            extends
            ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

        private int topSize = 4;

        public TopNAllFunction(int topSize) {

            this.topSize = topSize;
        }

        public void process(

                ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>.Context arg0,
                Iterable<Tuple2<String, Integer>> input,
                Collector<String> out) throws Exception {

            TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
                    new Comparator<Integer>() {

                        @Override
                        public int compare(Integer y, Integer x) {
                            return (x < y) ? -1 : 1;
                        }

                    }); //treemap按照key降序排列，相同count值不覆盖

            for (Tuple2<String, Integer> element : input) {
                treemap.put(element.f1, element);
                if (treemap.size() > topSize) { //只保留前面TopN个元素
                    treemap.pollLastEntry();
                }
            }

            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
                    .entrySet()) {
                out.collect("=================\n热销图书列表:\n" + new Timestamp(System.currentTimeMillis()) + treemap.toString() + "\n===============\n");
            }

        }

    }
}
