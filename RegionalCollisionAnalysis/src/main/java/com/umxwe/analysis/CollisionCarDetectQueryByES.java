
/**
 * Project Name:flink-es-sink-demo File Name:ESQueryDemo.java Package Name:com.coomia.query
 * Date:2020年9月9日下午5:25:37 Copyright (c) 2020, spancer.ray All Rights Reserved.
 *
 */

package com.umxwe.analysis;

import com.umxwe.common.elastic.bitmap.BitmapAggregationBuilder;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

/**
 * 车辆碰撞分析
 * select plateNo from t where shotTime between t1 and t2 and DeviceID in (d1,d2,d3) group by
 * DeviceID, PlateNo having count(PlateNo) > 1
 */

public class CollisionCarDetectQueryByES {

  public static void main(String[] args) throws IOException {

    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
    httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
    httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
    httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
    httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));


    RestHighLevelClient client =
        new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
    SearchSourceBuilder ssb = new SearchSourceBuilder();
    /**
     * query devices in areaA & areaB, along with time range in start & end.
     */
    long start = 1610725815509L; // time start in mills
    long end = 1611827712261L; // time end in mills
    String[] areaA = new String[] {"431200000011900007", "431200000011900008"}; // deviceIDs of areaA
    String[] areaB = new String[] {"4312000000119000026", "4312000000119000030"}; // deviceIDs of areaB

    QueryBuilder orQuery =
        QueryBuilders.boolQuery().should(QueryBuilders.termsQuery("deviceID", areaA))
            .should(QueryBuilders.termsQuery("deviceID", areaB));
    QueryBuilder query = QueryBuilders.boolQuery()
        .must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end)).must(orQuery);

    ssb.query(query);
    /**
     * calculate the  two result data (two collections), and get the intersection of the both.
     */
//    Map<String, Object> params = new HashMap<String, Object>();
//    params.put("areaA", new ArrayList<String>(Arrays.asList(areaA)));
//    params.put("areaB", new ArrayList<String>(Arrays.asList(areaB)));
//    Script init = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
//        " state.a1 =[]; state.a2 =[];", params);
//    Script map = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
//        "if(params.areaA.contains(doc.DeviceID.value.intValue()))state.a1.add(doc.PlateNo.value); else if(params.areaB.contains(doc.DeviceID.value.intValue())) state.a2.add(doc.PlateNo.value);",
//        params);
//    Script combine = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
//        "List profit = []; List profit2 = [];  for (t in state.a1) { profit.add(t)}  for (t in state.a2) { profit2.add(t)} profit.retainAll(profit2) ; return new HashSet(profit).toArray();",
//        params);
//    Script reduce = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
//        "List a=[];  for(s in states ) { for(t in s){ a.add(t)} }  return new HashSet(a);", params);
//    ScriptedMetricAggregationBuilder smab = AggregationBuilders.scriptedMetric("CollisionCar")
//            .initScript(init)
//            .mapScript(map)
//            .combineScript(combine)
//            .reduceScript(reduce);

    BitmapAggregationBuilder bitmapAggregationBuilder=new BitmapAggregationBuilder("bitmapAggregation");
    bitmapAggregationBuilder.field("plateNo");
    //AggregationBuilders.terms("GroupbydeviceID").field("deviceID")
//.subAggregation(bitmapAggregationBuilder)
//    AggregationBuilders.count("count").field("plateNo")
    ssb.size(0);
//    ssb.aggregation(AggregationBuilders.terms("GroupbydeviceID").field("deviceID").subAggregation(bitmapAggregationBuilder));
    ssb.aggregation(bitmapAggregationBuilder);
    System.out.println(ssb.toString());
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(ssb);
    searchRequest.indices("vehicletrajectoryentity"); //index name
    SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
    SearchHits hits = sr.getHits();
    long took = sr.getTook().getMillis();
    TotalHits totalHits = hits.getTotalHits();
    System.out.println("耗时：" + took + "ms,count:" + totalHits.value);

//    Aggregations aggRes = sr.getAggregations();
//    ParsedScriptedMetric devices = aggRes.get("GroupbydeviceID");
//    Object result = devices.aggregation();
//    if (null != result) {
//      List<Object> data = (List) result;
//      data.forEach(item -> System.out.println(item));
//    }

    //拿到响应的匹配结果,遍历
//    for (SearchHit hit : hits) {
//      //转为String,也可以getSourceAsMap转为map,后续进行操作
//      System.out.println("id: " + hit.getId() + " " + hit.getSourceAsString());
//    }
    client.close();
  }

}

