package com.ng.flink;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Flink 读取HBase 数据
 * @Author: Cedaris
 * @Date: 2019/6/24 9:54
 */
public class FlinkReadHbase {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
        tableEnvironment*/

        //读取方法一
        env.createInput(new TableInputFormat<Tuple2<String,String>>() {
            private String columnFamily = null;
            @Override
            public void configure(Configuration parameters) {
                super.configure(parameters);
                //配置HBase连接参数
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.property.clientPort","2181");
                conf.set("hbase.zookeeper.quorum","psy831,psy832,psy833");
                //创建HBase连接
                Connection conn = null;
                try {
                    conn = ConnectionFactory.createConnection(conf);
                    //获取表
                    Table table = conn.getTable(TableName.valueOf("tableName"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //设置扫描器
                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes("1001"));
                scan.setStopRow(Bytes.toBytes("1004"));
                scan.addFamily(Bytes.toBytes(columnFamily));
            }

            @Override
            protected Scan getScanner() {
                return scan;
            }

            @Override
            protected String getTableName() {
                return table.getName().toString();
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String rowkey = Bytes.toString(result.getRow());
                StringBuffer sb = new StringBuffer();
                for (Cell cell : result.rawCells()) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength());
                    sb.append(value).append(",");
                }
                String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
                Tuple2<String, String> tuple2 = new Tuple2<>();
                tuple2.setField(rowkey,0);
                tuple2.setField(valueString,1);
                return tuple2;
            }

            @Override
            public void close() throws IOException {
                try{
                    if(table != null){
                        table.close();
                    }

                }catch (IOException e){
                    System.out.println(e.getMessage());
                }
            }
        });

        //读取方法二
        DataStreamSource<Tuple2<String, String>> tuple2DataStreamSource = env.addSource(new HBaseReader());
    }

}
