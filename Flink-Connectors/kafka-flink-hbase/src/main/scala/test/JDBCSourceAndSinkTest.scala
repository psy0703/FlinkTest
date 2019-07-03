package test

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

object JDBCSourceAndSinkTest {

  def main(args: Array[String]): Unit = {

    //1.创建流执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val strenv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment



    val batchTableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val streamTableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(strenv)



    readFromMysql(strenv)



  }



  def readFromMysql(streamenv:StreamExecutionEnvironment): Unit={

    //2.从自定义source中读取数据
    val students: DataStream[Student] = streamenv.addSource(new JDBCReader)

    //3.显示结果
    students.print().setParallelism(1)


    //4.触发流执行
    streamenv.execute("MySourceJob")

  }



    def readFromHdfsToMysql(env:ExecutionEnvironment,batchTableEnv: BatchTableEnvironment,streamTableEnv: StreamTableEnvironment,hdfsDataPath:String): Unit={

      val student_dataSet: DataSet[Student] = env.readCsvFile[Student](
        filePath = hdfsDataPath,
        lineDelimiter = "\n",
        fieldDelimiter = ",",
        lenient = false,
        ignoreFirstLine = true,
        includedFields = Array(0, 1, 2, 3, 4),
        pojoFields = Array("id", "name", "sex", "age", "department")
      )

      //转化dataSet为Table
      val student_table: Table = batchTableEnv.fromDataSet(student_dataSet)

      student_table.printSchema()

      //转化Table为dataStream
      val student_dataStream: DataStream[Student] = streamTableEnv.toAppendStream[Student](student_table)

      //调用自定义Sink写入Mysql
      student_dataStream.addSink(new JDBCSink)

      env.execute()



    }





}
