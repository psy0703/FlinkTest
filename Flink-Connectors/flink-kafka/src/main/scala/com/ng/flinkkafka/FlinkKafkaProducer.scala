package com.ng.flinkkafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Flink 添加Kafka Producer
  * @Author: Cedaris
  * @Date: 2019/7/2 10:24
  */
object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    val sourceStream: DataStreamSource[String] = env.fromElements("3816678,,,,54116,2,,44,4451,445103,445103115,广东省,潮州市," +
      "潮安区,归湖镇,,,,,,,,,,,中国有个寺庙很奇怪，被一块2万吨巨石压了555年,,,2019-05-2910:20:48,,," +
      "http://jxtt.diangoumall" +
      ".com/eb861e58vodtranscq1258058953/7fe923bf5285890789625563954" +
      "/1559096537_3835984447.100_0.jpg,,,2019-06-0323:58:02,,,,,,,,,,,")

    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"psy831:9092")
    val topic = "hometown_headline"
    val producer = new FlinkKafkaProducer010[String](topic,new
        SimpleStringSchema(),props)

    ////event-timestamp事件的发生时间
    producer.setWriteTimestampToKafka(true)

    sourceStream.addSink(producer)

    env.execute()
  }

}
