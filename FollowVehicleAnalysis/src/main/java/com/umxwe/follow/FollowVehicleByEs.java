package com.umxwe.follow;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.TimeRangeExistQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FollowVehicleByEs
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/31
 */
public class FollowVehicleByEs {

    public static void main(String[] args) throws IOException {

        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));

        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.should(QueryBuilders.existsQuery("k1"));
        boolQuery.should(QueryBuilders.existsQuery("k2"));
        boolQuery.should(QueryBuilders.existsQuery("k3"));
        boolQuery.should(QueryBuilders.existsQuery("k4"));
        boolQuery.minimumShouldMatch(2);
        boolQuery.mustNot(QueryBuilders.termQuery("plateNo", "湘A4NS91"));

/**
 *  "k1^99,51,11,33",
 *         "k2^12,199,55,66",
 *         "k3^149,14,24,34",
 *         "k4^500,501,502,503"
 */
        Map<String, String> fieldsValues = new HashMap<>();
        fieldsValues.put("k1", "99,51,11,33");
        fieldsValues.put("k2", "12,199,55,66");
        fieldsValues.put("k3", "149,14,24,34");
        fieldsValues.put("k4", "500,501,502,503");
        Integer minMatch = 2;
        Long timeInterval = 20L;
        TimeRangeExistQueryBuilder timeRangeExistQueryBuilder = new TimeRangeExistQueryBuilder(fieldsValues, minMatch, timeInterval);
        ssb.query(boolQuery);
        ssb.postFilter(timeRangeExistQueryBuilder);
        System.out.println(ssb.toString());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(ssb);
        searchRequest.indices("car2022");
        SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
        long took = sr.getTook().getMillis();
        System.out.println("耗时：" + took);
        Iterator<SearchHit> iterator = sr.getHits().iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            System.out.println(hit.toString());
        }
        client.close();
    }
}
