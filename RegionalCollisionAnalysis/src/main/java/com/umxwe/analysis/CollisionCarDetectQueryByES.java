
/**
 * Project Name:flink-es-sink-demo File Name:ESQueryDemo.java Package Name:com.coomia.query
 * Date:2020年9月9日下午5:25:37 Copyright (c) 2020, spancer.ray All Rights Reserved.
 */

package com.umxwe.analysis;

import com.umxwe.common.elastic.bitmap.BitmapUtil;
import com.umxwe.elasticsearchplugin.bitmap.BitmapAggregationBuilder;
import com.umxwe.elasticsearchplugin.bitmap.ParsedBitmap;
import com.umxwe.umxmax.MaxAggregationBuilder;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 车辆碰撞分析
 * select plateNo from t where shotTime between t1 and t2 and DeviceID in (d1,d2,d3) group by
 * DeviceID, PlateNo having count(PlateNo) > 1
 */

public class CollisionCarDetectQueryByES {

    public static void main(String[] args) throws IOException {

        List<HttpHost> httpHosts = new ArrayList<>();
//    httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
//    httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
//    httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
//    httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
//    httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));

        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));


        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
        /**
         * query devices in areaA & areaB, along with time range in start & end.
         */
        long start1 = 1609431788104L; // time start in mills
        long end1 = 1611222777174L; // time end in mills
        QueryBuilder query1 = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("shotTime").gte(start1).lte(end1));

        long start2 = 1609431788104L; // time start in mills
        long end2 = 1611222777174L; // time end in mills
        QueryBuilder query2 = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("shotTime").gte(start2).lte(end2));



        long currentTime=System.currentTimeMillis();
        Roaring64Bitmap roaring64Bitmap1=distinctPlateNoByBitmap(client,null);
        Roaring64Bitmap roaring64Bitmap2=distinctPlateNoByBitmap(client,null);
        long currentInsectionTime=System.currentTimeMillis();
        roaring64Bitmap1.and(roaring64Bitmap2);

        System.out.println("insection_size:"+roaring64Bitmap1.getLongCardinality()+"，insection_time:"+(System.currentTimeMillis()-currentInsectionTime)+" ms,两个区域求交集总耗时："+(System.currentTimeMillis()-currentTime)+" ms");

        client.close();
    }

    public static Roaring64Bitmap distinctPlateNoByBitmap(RestHighLevelClient client, QueryBuilder query) throws IOException {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        if(query!=null){
            ssb.query(query);
        }
        BitmapAggregationBuilder bitmapAggregationBuilder = new BitmapAggregationBuilder("umxbitmap");
        bitmapAggregationBuilder.field("plateNo");

        ssb.size(0);
        ssb.aggregation(bitmapAggregationBuilder);
        System.out.println("ES_DSL:" + ssb.toString());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(ssb);
        //index: vehicletrajectoryentity carspeedindex carws car
        searchRequest.indices("carspeedindex"); //index name
        SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = sr.getHits();
        long took = sr.getTook().getMillis();
        TotalHits totalHits = hits.getTotalHits();
        System.out.println("单次查询耗时：" + took + " ms,totalHits:" + totalHits.value);

        Aggregations aggRes = sr.getAggregations();
        ParsedBitmap aggResTerm = aggRes.get("umxbitmap");
        Roaring64Bitmap roaring64Bitmap = BitmapUtil.deserializeBitmap(aggResTerm.getBitmapByte());
        System.out.println(roaring64Bitmap.getLongCardinality());
        return roaring64Bitmap;
    }

}

