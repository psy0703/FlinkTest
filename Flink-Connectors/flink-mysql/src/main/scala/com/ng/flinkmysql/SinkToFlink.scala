package com.ng.flinkmysql

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.PropertyResourceBundle

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * @Author: Cedaris
  * @Date: 2019/6/24 16:14
  */
class SinkToFlink extends RichSinkFunction[Province]{
  private var connection: Connection = null
  private var ps: PreparedStatement = null
  private var sql: String = "insert into province(stuid,stuname,stuaddr,stusexvalues(?,?,?,?);"

  /**
    * open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
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
    * 每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
    * @param value
    */
  override def invoke(value: Province): Unit = {
    ps.setString(1,value.id);
    ps.setString(2,value.name)
  }

  override def close(): Unit = {
    //5.关闭连接和释放资源
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}
