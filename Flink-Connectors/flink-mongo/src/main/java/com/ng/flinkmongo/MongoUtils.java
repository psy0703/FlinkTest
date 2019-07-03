package com.ng.flinkmongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Cedaris
 * @Date: 2019/6/25 17:38
 */
public class MongoUtils {
    public static MongoClient getConnect(){
        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        ArrayList<MongoCredential> credentials = new ArrayList<>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential credential = MongoCredential.createCredential("psy831", "test",
                "root".toCharArray());
        credentials.add(credential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(serverAddress, credentials);
        return mongoClient;
    }
}
