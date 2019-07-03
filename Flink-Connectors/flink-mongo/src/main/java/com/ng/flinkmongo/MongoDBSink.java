package com.ng.flinkmongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Cedaris
 * @Date: 2019/6/25 17:43
 */
public class MongoDBSink extends RichSinkFunction<Tuple2<String,String>> {
    private static final long serialVersionUID = 1L;
    MongoClient mongoClient = null;

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if(mongoClient != null){
            mongoClient = MongoUtils.getConnect();
            MongoDatabase database = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = database.getCollection("kafka");

            List<Document> list = new ArrayList<>();
            Document doc = new Document();
            doc.append("IP",value.f0);
            doc.append("Time",value.f1);
            list.add(doc);
            System.out.println("Insert Starting");
            collection.insertMany(list);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoUtils.getConnect();
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
