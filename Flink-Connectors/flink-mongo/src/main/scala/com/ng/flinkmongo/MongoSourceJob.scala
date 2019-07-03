package com.ng.flinkmongo

import com.mongodb.hadoop.io.BSONWritable
import com.mongodb.hadoop.mapred.MongoInputFormat
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat
import org.apache.hadoop.mapred.JobConf
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author: Cedaris
  * @Date: 2019/6/25 14:39
  */
object MongoSourceJob {

  private val logger: Logger = LoggerFactory.getLogger(classOf[MongoSourceJob])
  private val MONGO_URI = "mongodb://ip:port/db.collection"

  def main(args: Array[String]): Unit = {
    //获取条件参数
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val webSource: String = tool.get("webSource","baidu")
    val year: Int = tool.getInt("year",2019)
    val condition: String = String.format("{'source':'%s','year':{'$regex':'^%d'}}",
      webSource,
      year)
    //创建运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //将mongo数据转化为Hadoop数据格式
    val hdif = new HadoopInputFormat[BSONWritable, BSONWritable](new
        MongoInputFormat(),
      classOf[BSONWritable],
      classOf[BSONWritable],
      new JobConf)

    hdif.getJobConf.set("mongo.input.split.create_input_splits","false")
    hdif.getJobConf.set("mongo.input.uri",MONGO_URI)
    hdif.getJobConf.set("mongo.input,query",condition)

    val datasource: DataSource[(BSONWritable, BSONWritable)] = env.createInput(hdif)
    val count: Long = datasource.map(MapFunction[Tuple2[BSONWritable, BSONWritable], String])
      .count()

    logger.info("总共读取到{}条MongoDB数据",count)
  }
}
