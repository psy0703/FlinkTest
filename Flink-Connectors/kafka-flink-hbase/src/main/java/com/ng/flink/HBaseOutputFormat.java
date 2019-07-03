package com.ng.flink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: Cedaris
 * @Date: 2019/6/24 10:20
 */
public class HBaseOutputFormat implements OutputFormat<Tuple2<String,String>> {
    private static final Logger logger =
            LoggerFactory.getLogger(HBaseOutputFormat.class);
    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private String tableName = null;
    private String columnFamily = null;

    @Override
    public void configure(Configuration configuration) {
//配置HBase连接参数

        //设置扫描器
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1004"));
        scan.addFamily(Bytes.toBytes(columnFamily));
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
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
    }

    @Override
    public void writeRecord(Tuple2<String, String> record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.f0));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("test"),
                Bytes.toBytes(record.f1));
        table.put(put);
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
}
