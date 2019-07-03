package t2

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.log4j.{Level, Logger}
import test.Student

object MyJDBCSinkTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "psy831")
    lazy val logger = Logger.getLogger(MyJDBCSinkTest.getClass)

    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.flink").setLevel(Level.WARN)
    Logger.getLogger("org.flink_project").setLevel(Level.WARN)
    //1.创建流执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val strenv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    strenv.enableCheckpointing(5000)
    strenv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    strenv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val batchTableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val streamTableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(strenv)

  /*
     //测试结果OK
     val value: DataStream[Student] = strenv.fromElements(
      new Student(5, "dahua", "男", 37, "female"),
      new Student(6, "daming", "女", 23, "male "),
      new Student(7, "daqiang ", "男", 12, "female")
    )
    value.addSink(new JDBCSink)

*
    val hdfsDataPath = "hdfs://psy831:9000/student.csv"

    //测试结果 OK
    val value: DataStream[String] = strenv.readTextFile("hdfs://tqbs/flink_test/student.txt")

    val student_datastream: DataStream[Student] = value.map(row => {
      val strings = row.split(",")
      val id: Int = strings(0).toInt
      val name: String = strings(1)
      val sex: String = strings(2)
      val age: Int = strings(3).toInt
      val department: String = strings(4)
      new Student(id, name, sex, age, department)
    })
    student_datastream.addSink(new JDBCSink)*/



    val student_dataSet: DataSet[Student] = env.readCsvFile[Student](
      filePath = "hdfs://psy831:9000/student.csv",
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      lenient = false,
      ignoreFirstLine = true,
      includedFields = Array(0, 1, 2, 3, 4),
      pojoFields = Array("id", "name", "sex", "age", "department")
    )
    student_dataSet.print()
    println("-------------------------------------------")

    val sream: DataStream[Student] = strenv.fromCollection(
      student_dataSet.collect()
    )
    //转化dataSet为Table
    val student_table: Table = batchTableEnv.fromDataSet[Student](student_dataSet)

    student_table.printSchema()

    sream.addSink(new JDBCSink2)
    //转化Table为dataStream
    //Exception in thread "main" java.lang.AssertionError: Relational expression rel#6:LogicalTableScan.NONE(table=[_DataSetTable_0]) belongs to a different planner than is currently being used.
//    val student_dataStream: DataStream[(String,String,String,String,String)] = streamTableEnv.toAppendStream[(String,String,String,String,String)](student_table)

    //val student_dataStream: DataStream[(Boolean, Student)] = streamTableEnv.toRetractStream[Student](student_table)


    //调用自定义Sink写入Mysql
//    student_dataStream.addSink(new JDBCSink2)
//    env.execute()
    strenv.execute()
  }

}
