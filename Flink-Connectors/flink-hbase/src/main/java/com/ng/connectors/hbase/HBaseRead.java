package com.ng.connectors.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Flink 读取Hbase数据
 * @Author: Cedaris
 * @Date: 2019/6/20 14:20
 */
public class HBaseRead {
    //table name
    public static final String HBASE_TABLE_NAME = "t_user";
    //column family
    static final byte[] COLUMNFAMILY =
            "info".getBytes(ConfigConstants.DEFAULT_CHARSET);
    //column name
    static final byte[] COLUMN =
            "bar".getBytes(ConfigConstants.DEFAULT_CHARSET);

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.createInput(new TableInputFormat<Tuple2<String,String>>() {

            private Tuple2<String,String> reuse = new Tuple2<String,String>();

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(COLUMNFAMILY,COLUMN);
                return scan;
            }

            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
                String val = Bytes.toString(result.getValue(COLUMNFAMILY, COLUMN));
                reuse.setField(key,0);
                reuse.setField(val,1);
                return reuse;
            }
        }).filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1.startsWith("ng");
            }
        }).print();
    }
}
