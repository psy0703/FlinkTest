package com.ng.flinkmysql

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.PropertyResourceBundle
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * 自定义Mysql的Source
  * @Author: Cedaris
  * @Date: 2019/6/24 15:48
  */
class SourceFromMysql extends RichSourceFunction[Province] {
  private var connection: Connection = null
  private var ps: PreparedStatement = null
  private var sql: String = "select * from province"

  /**
    * open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val stream = new FileInputStream("Flink-Connectors/flink-mysql/src/main/resources/mysql.properties")
    val bundle = new PropertyResourceBundle(stream)
    val username: String = bundle.getString("username")
    val password: String = bundle.getString("password")
    val driver: String = bundle.getString("driver")
    val url: String = bundle.getString("url")

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    ps = connection.prepareStatement(sql)
  }

  /**
    * DataStream调用一次run()方法用来获取数据
    *
    * @param ctx
    */
  override def run(ctx: SourceFunction.SourceContext[Province]): Unit = {
    try {
      //执行查询，封装数据
      val set: ResultSet = ps.executeQuery()
      while (set.next()) {
        val province = Province(set.getString("province_code"), set.getString("name"))
        ctx.collect(province)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }


  /**
    * 程序执行完毕就可以进行，关闭连接和释放资源的动作
    */
  override def cancel(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}
