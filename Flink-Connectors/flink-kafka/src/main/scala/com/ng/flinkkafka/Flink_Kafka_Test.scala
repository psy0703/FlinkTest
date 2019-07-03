/*
package com.ng.flinkkafka

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

/**
  * @Author: Cedaris
  * @Date: 2019/6/24 13:52
  */
object Flink_Kafka_Test {

  def main(args: Array[String]): Unit = {
    kafkaConsumer
  }

  def kafkaProducer(): Unit ={

    val stream: DataStream[String] = null
    val myProducer = new FlinkKafkaProducer011[String](
      "psy831:9092,psy832:9092,psy833:9092",
      "headline",
      new SimpleStringSchema
    )
    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)

    stream.addSink(myProducer)
  }

  def kafkaConsumer(): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","psy831:9092,psy832:9092,psy833:9092")
    properties.setProperty("zookeeper.connect","psy831:2181,psy832:2181,psy833:2181")
    properties.setProperty("group_id","test")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000) // checkpoint every 5000 msecs

    val myConsumer = new FlinkKafkaConsumer011[String](
      java.util.regex.Pattern.compile("test-topic-[0-9]"))
//    myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter())
    myConsumer.setStartFromEarliest()// start from the earliest record possible
//    myConsumer.setStartFromLatest() // start from the earliest record possible
//    myConsumer.setStartFromTimestamp(2013141516)// start from specified epoch
    // timestamp (milliseconds)
//    myConsumer.setStartFromGroupOffsets()  // the default behaviour

    /*val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)
*/
//    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)


    val stream: DataStreamSource[String] = env.addSource(new FlinkKafkaConsumer011[String]("topic", new
        SimpleStringSchema(), properties))

    val unit: DataStreamSink[String] = stream.print()

    env.execute()
  }

}
*/
