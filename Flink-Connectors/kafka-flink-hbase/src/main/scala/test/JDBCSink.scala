package test

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class JDBCSink extends RichSinkFunction[Student]{

  var ps: PreparedStatement = _
  private var connection: Connection = _

  /**
    * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
    */
  override def open(parameters: Configuration): Unit ={

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
    val sql = "insert into student values(?,?,?,?,?);"
    ps = connection.prepareStatement(sql)

  }

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    val id: Int = value.getId
    val name = value.getName
    val sex = value.getSex
    val age = value.getAge
    val department = value.getDepartment

    ps.setInt(1,id)
    ps.setString(2,name)
    ps.setString(3,sex)
    ps.setInt(4,age)
    ps.setString(5,department)

    ps.execute()



  }

  override def close(): Unit = super.close()
  super.close
  if (connection != null) connection.close
  if (ps != null) ps.close

}

