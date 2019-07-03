package com.ng.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 写入HBase
 * 第一种：继承RichSinkFunction重写父类方法
 *
 * 注意：由于flink是一条一条的处理数据，所以我们在插入hbase的时候不能来一条flush下，
 * 不然会给hbase造成很大的压力，而且会产生很多线程导致集群崩溃，所以线上任务必须控制flush的频率。
 * 解决方案：我们可以在open方法中定义一个变量，
 * 然后在写入hbase时比如500条flush一次，或者加入一个list，判断list的大小满足某个阀值flush一下
 * @Author: Cedaris
 * @Date: 2019/6/24 11:19
 */
public class HBaseWriter extends RichSinkFunction<String> {
    private Connection conn = null;
    private Scan scan = null;
    private BufferedMutator mutator = null;
    private int count = 0;
    private String tableName = null;
    private String columnFamily = null;

    /**
     * 建立HBase连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.zookeeper.quorum","psy831,psy832,psy833");
        //创建HBase连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024);  //设置缓存的大小
        mutator = conn.getBufferedMutator(params);
        count = 0;
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] array = value.split(",");
        Put put = new Put(Bytes.toBytes(array[0]));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("name"),
                Bytes.toBytes(array[1]));
        mutator.mutate(put);
        //每满2000条刷新一下数据
        if(count >= 2000){
            mutator.flush();
            count = 0;
        }
    }

    /**
     * 关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (conn != null) conn.close();
    }
}
