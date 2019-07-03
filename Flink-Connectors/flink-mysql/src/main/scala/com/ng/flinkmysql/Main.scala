package com.ng.flinkmysql

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * @Author: Cedaris
  * @Date: 2019/6/24 16:01
  */
object Main {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStreamSource[Province] = env.addSource(new SourceFromMysql() )
    ds.print()
    env.execute()

    env.clean()
  }

  def writedataToMysql(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStreamSource[Province] = env.fromElements(
      Province("11", "beijing"),
      Province("20", "shanghai")
    )
    dataStream.addSink(new SinkToFlink)

    env.execute()
  }
}
