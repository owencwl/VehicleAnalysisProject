package com.umxwe.analysis;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName IntoUrbanAnalysis
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/16
 */
public class IntoUrbanAnalysis {

    public static void main(String[] args) {
        /**
         * 创建es的集群服务器地址
         */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));

        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));

        long start1 = 1609431788104L; // time start in mills
        long end1 = 1611222777174L; // time end in mills
        QueryBuilder query1 = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("shotTime").gte(start1).lte(end1));

        QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("",""));
    }
}
