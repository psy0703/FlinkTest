package com.ng.flinkmysql

import java.io.FileInputStream
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{PropertyResourceBundle, ResourceBundle}

/**
  * @Author: Cedaris
  * @Date: 2019/6/24 15:34
  */
object jdbcTest {
  def main(args: Array[String]): Unit = {
    val stream = new FileInputStream("Flink-Connectors/flink-mysql/src/main/resources/mysql.properties")
    val bundle = new PropertyResourceBundle(stream)
    val username: String = bundle.getString("username")
    val password: String = bundle.getString("password")
    val driver: String = bundle.getString("driver")
    val url: String = bundle.getString("url")

    Class.forName(driver)
    val conn: Connection = DriverManager.getConnection(url,username,password)
    val statement: Statement = conn.createStatement()
    val resultSet: ResultSet = statement.executeQuery("select * from province")
    while(resultSet.next()){
      val pro = Province(resultSet.getString("province_code"),resultSet
        .getString
      ("name"))
      println(pro)
    }

    if(conn != null){
      conn.close()
    }
  }

}
