package com.ng.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * 读HBase提供两种方式，一种是继承RichSourceFunction，重写父类方法，
 * @Author: Cedaris
 * @Date: 2019/6/24 9:34
 */
public class HBaseReader extends RichSourceFunction<Tuple2<String,String>> {
    private static final Logger logger =
            LoggerFactory.getLogger(HBaseReader.class);

    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private String tableName = null;
    private String columnFamily = null;

    /**
     * 在open方法使用HBase的客户端连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //配置HBase连接参数
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.zookeeper.quorum","psy831,psy832,psy833");
        //创建HBase连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        //设置扫描器
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1004"));
        scan.addFamily(Bytes.toBytes(columnFamily));

    }

    /**
     * run方法来自java的接口文件SourceFunction
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            sourceContext.collect(new Tuple2(rowkey,valueString));
        }

    }

    @Override
    public void cancel() {
        //关闭hbase的连接，关闭table表
        try{
            if(table != null){
                table.close();
            }
            if(conn != null){
                conn.close();
            }
        }catch (IOException e){
            logger.error("Close Hbase Exception: ", e.toString());
        }
    }
}
