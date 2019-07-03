package test

import java.sql._

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class JDBCReader extends RichSourceFunction[Student]{

  var ps: PreparedStatement = _
  private var connection: Connection = _

  /**
    * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hdp01:3306/tb_mall_headline"
    val username = "root"
    val password = "123456"
    //1.加载驱动
    Class.forName(driver)
    //2.创建连接
    connection = DriverManager.getConnection(url, username, password)
    //3.获得执行语句
    val sql = "select id,name,sex,age, department from student;"
    ps = connection.prepareStatement(sql)
  }

  override def cancel(): Unit = {
    println("取消")
  }

  /**
    * 二、DataStream调用一次run()方法用来获取数据
    */
  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    try { //4.执行查询，封装数据
      val resultSet = ps.executeQuery
      while (resultSet.next) {
        val student = new Student(
          resultSet.getInt("id"),
          resultSet.getString("name").trim,
          resultSet.getString("sex").trim,
          resultSet.getInt("age"),
          resultSet.getString("department").trim)
        sourceContext.collect(student)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  /**
    * 三、 程序执行完毕就可以进行，关闭连接和释放资源的动作了
    */
  override def close(): Unit = { //5.关闭连接和释放资源
    super.close
    if (connection != null) connection.close
    if (ps != null) ps.close
  }

}

