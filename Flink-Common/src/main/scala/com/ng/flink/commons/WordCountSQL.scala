package com.ng.flink.commons

import java.util

import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.java.BatchTableEnvironment

/**
  * @Author: Cedaris
  * @Date: 2019/7/3 11:11
  */
object WordCountSQL1 {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val wcs = new util.ArrayList[WC]()
    val wordStr = "Hello Flink Hello TOM"
    val words: Array[String] = wordStr.split("\\W+")
    for (elem <- words) {
      val wc = new WC(elem,1)
      wcs.add(wc)
    }

    val input: DataSource[WC] = env.fromCollection(wcs)
    tEnv.registerDataSet("WordCount",input,"word, frequency")
    val table: Table = tEnv.sqlQuery(
      "select word,SUM(frequency) as frequency from WordCount group by word"
    )
    val result: DataSet[WC] = tEnv.toDataSet(table,classOf[WC])
    result.print()

  }
  class WC(word:String,frequency:Long){
    this.word
    this.frequency

    override def toString: String = {
      "WC " + word + " " + frequency
    }
  }
  object WC{

  }
}

