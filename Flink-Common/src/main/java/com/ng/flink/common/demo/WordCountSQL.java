package com.ng.flink.common.demo;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Cedaris
 * @Date: 2019/7/3 11:42
 */
public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        List list = new ArrayList();
        String wordsStr = "Hello Flink Hello TOM";
        String[] words = wordsStr.split("\\W+");
        for(String word : words){
            WC wc = new WC(word, 1);
            list.add(wc);
        }
        DataSet<WC> input = env.fromCollection(list);
        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WC> result = tEnv.toDataSet(table, WC.class);
        result.print();

    }
    public static class WC {
        public String word;//hello
        public long frequency;//1

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}


