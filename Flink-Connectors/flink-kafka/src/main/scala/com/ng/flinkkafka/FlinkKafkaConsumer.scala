package com.ng.flinkkafka

import java.util.Properties

import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFlatMapFunction, RichFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
  * @Author: Cedaris
  * @Date: 2019/7/1 8:47
  */
object FlinkKafkaConsumer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "psy831:9092,psy832:9092,psy833:9092")
    props.setProperty("group.id", "test")

    val topic = "hometown_headline"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props)
    consumer.setStartFromEarliest()

    val stream: DataStream[String] = env.addSource(consumer)


    val value: SingleOutputStreamOperator[(String, Long)] = stream.flatMap(new RichFlatMapFunction[String, Tuple2[String, Long]] {
      override def flatMap(value: String, out: Collector[(String, Long)])
      : Unit = {
        val logs: Array[String] = value.split(",", 46)
        val userId: String = logs(0)
        val tuple: (String, Long) = (userId, 1L)
        println(System.currentTimeMillis()  + " : " + tuple.toString())
        out.collect(tuple)
      }
    })



    /*value.keyBy(0)
      .timeWindow(Time.seconds(5), Time.seconds(2))
      .sum(1).print()*/

    env.execute("Flink-Kafka")
  }

  case class WordCount(word: String, count: Long) {
    override def toString: String = {
      return "WordWithCount{" +
        "word='" + word + '\'' +
        ", count=" + count +
        '}';
    }
  }

}
