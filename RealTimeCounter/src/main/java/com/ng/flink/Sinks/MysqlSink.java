package com.ng.flink.Sinks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: Cedaris
 * @Date: 2019/7/3 16:20
 */
public class MysqlSink extends RichSinkFunction<Tuple2<String,Integer>> {
    private Connection conn;
    private static PreparedStatement prep;
    private static final String username = "root";
    private static final String password = "root";
    private static final String driverName = "com.mysql.jdbc.Driver";
    private static final String dbUri = "jdbc:mysql://192.168.60.131/test";

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(dbUri,username,password);
//        String sql = "replace into bookSale(name,count) values(?,?)";
        String sql = "update booksale set count = ? where name = ?";
        prep = conn.prepareStatement(sql);
        prep.setString(2,value.f0);
        prep.setInt(1,value.f1);

        prep.executeUpdate();
        if(prep != null){
            prep.close();
        }
        if (conn != null){
            conn.close();
        }
    }
}
