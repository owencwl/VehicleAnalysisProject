package com.umxwe.follow;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName GenetateData
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/30
 */
public class GenetateData {
    public static void main(String[] args) throws IOException {
        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));

        /**
         * 创建es的索引，索引之前存在则会删除重建
         */
        String esIndex = "car2022";
        createESIndex(httpHosts, esIndex);


    }

    public static void createESIndex(List<HttpHost> httpHosts, String index) throws IOException {
        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
        boolean recreate = true;

        // delete index
        if (recreate && !client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
//            DeleteIndexRequest deleteIndex = new DeleteIndexRequest(index);
//            client.indices().delete(deleteIndex, RequestOptions.DEFAULT);

            // mapping config and put
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("plateNo");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("k1");
                    {
                        builder.field("type", "text");
                        builder.field("store", true);
                    }
                    builder.endObject();

                    builder.startObject("k2");
                    {
                        builder.field("type", "text");
                        builder.field("store", true);
                    }
                    builder.endObject();
                    builder.startObject("k3");
                    {
                        builder.field("type", "text");
                        builder.field("store", true);
                    }
                    builder.endObject();
                    builder.startObject("k4");
                    {
                        builder.field("type", "text");
                        builder.field("store", true);
                    }
                    builder.endObject();


                }
                builder.endObject();
            }
            builder.endObject();
            // create index car
            CreateIndexRequest createIndex = new CreateIndexRequest(index);
            createIndex.mapping(index, builder);
            createIndex.settings(
                    Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
            client.indices().create(createIndex, RequestOptions.DEFAULT);
        }
    }
}
