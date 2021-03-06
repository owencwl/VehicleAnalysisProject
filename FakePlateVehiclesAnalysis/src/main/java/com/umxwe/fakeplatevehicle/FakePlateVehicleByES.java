package com.umxwe.fakeplatevehicle;

import com.umxwe.common.elastic.distance.UmxMaxSpeedAggregationBuilder;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FakePlateVehicleByES @Description Todo @Author owen(umxwe) @Date 2021/2/23
 */
public class FakePlateVehicleByES {
    public static void main(String[] args) throws IOException {
        compute();
    }

    private static void compute() throws IOException {
        long start = 1610725815509L; // time start in mills
        long end = 1611827712261L; // time end in mills
        String plateNo = null;
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));

        RestHighLevelClient client =
                new RestHighLevelClient(
                        RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder query =
                QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("shotTime").gte(start).lte(end));
        if (null != plateNo) query.must(QueryBuilders.termsQuery("plateNo", plateNo));
        CardinalityAggregationBuilder color =
                AggregationBuilders.cardinality("plateColorDescDistinct").field("plateColorDesc");
        CardinalityAggregationBuilder clas =
                AggregationBuilders.cardinality("vehicleClassDescDistinct").field("vehicleClassDesc");
        CardinalityAggregationBuilder brand =
                AggregationBuilders.cardinality("VehicleBrandDistinct").field("vehicleBrand");
        Map<String, Object> speedParam = new HashMap<String, Object>();
        speedParam.put("speed", 80); // 时速为80公里每小时
        Script intervalInit =
                new Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "state.agg =new HashMap();", speedParam);
        Script intervalMap =
                new Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "state.agg.put(doc['shotTime'],doc['location'].value)",
                        speedParam);
        Script intervalCombine =
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "return state", speedParam);
        Script intervalReduce =
                new Script(
                        ScriptType.INLINE,
                        Script.DEFAULT_SCRIPT_LANG,
                        "HashMap d = new HashMap(); int result; for(s in states){d.putAll(s.agg)}  List keys = new ArrayList(d.keySet()); for (int i = 0; i < keys.size(); i++) {for (int j = i+1; j < keys.size(); j++){double lat1 = d.get(keys.get(i)).lat;double lon1 = d.get(keys.get(i)).lon;double lat2 = d.get(keys.get(j)).lat;double lon2 = d.get(keys.get(j)).lon;double TO_METERS = 6371008.7714D;double TO_RADIANS = Math.PI / 180D;double x1 = lat1 * TO_RADIANS;double x2 = lat2 * TO_RADIANS;double h1 = 1 - Math.cos(x1 - x2);double h2 = 1 - Math.cos((lon1 - lon2) * TO_RADIANS);double h = h1 + Math.cos(x1) * Math.cos(x2) * h2;double dist = TO_METERS * 2 * Math.asin(Math.min(1, Math.sqrt(h * 0.5))); double speed =dist*3600/Math.abs(keys.get(i).value-keys.get(j).value); if(speed>params.speed) {result=2; break;}}} return result;",
                        speedParam);
        /** 构建速度计算的脚本，如果速度大于配置的speedParam的值（默认80), 函数会返回2（为什么返回2是因为其它过滤都用了distinct查询，会统一查distinct>1) */
        //        ScriptedMetricAggregationBuilder interval =
        // AggregationBuilders.scriptedMetric("SpeedMetrics");
        //        interval.initScript(intervalInit);
        //        interval.mapScript(intervalMap);
        //        interval.combineScript(intervalCombine);
        //        interval.reduceScript(intervalReduce);
        /** build script and params. */
        Map<String, String> bucketsPathsMap = new HashMap<String, String>();
        bucketsPathsMap.put("plateColorDescDistinct", "plateColorDescDistinct");
        bucketsPathsMap.put("vehicleClassDescDistinct", "vehicleClassDescDistinct");
        bucketsPathsMap.put("VehicleBrandDistinct", "VehicleBrandDistinct");
        //        bucketsPathsMap.put("SpeedMetrics", "SpeedMetrics.value");
        Map<String, Object> havingScriptParam = new HashMap<String, Object>();
        havingScriptParam.put("havingCount", 1); // distinct count > 1，即表示有重复的数据（不同颜色或不同型号或其它）
        // || SpeedMetrics>havingCount
        Script script =
                new Script(
                        ScriptType.INLINE,
                        "expression",
                        "plateColorDescDistinct >havingCount || vehicleClassDescDistinct >havingCount || VehicleBrandDistinct > havingCount ",
                        havingScriptParam);
        BucketSelectorPipelineAggregationBuilder having =
                PipelineAggregatorBuilders.bucketSelector("HavingPlateNoGT1", bucketsPathsMap, script);

        List fields = new ArrayList<String>();
        fields.add("shotTime");
        fields.add("location");
        UmxMaxSpeedAggregationBuilder umxDistanceAggregationBuilder =
                new UmxMaxSpeedAggregationBuilder("speed").fields(fields);

        ssb.aggregation(
                AggregationBuilders.terms("GroupbyPlateNo")
                        .field("plateNo")
                        .subAggregation(color)
                        .subAggregation(clas)
                        .subAggregation(brand)
                        //                .subAggregation(interval)
                        .subAggregation(umxDistanceAggregationBuilder)
                        .subAggregation(having));
        ssb.query(query);
        ssb.size(0);
        System.out.println(ssb.toString());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(ssb);
        searchRequest.indices("vehicletrajectoryentity");
        SearchResponse sr = client.search(searchRequest, RequestOptions.DEFAULT);
        long took = sr.getTook().getMillis();
        System.out.println("耗时：" + took);

        Aggregations aggRes = sr.getAggregations();
        Terms devices = aggRes.get("GroupbyPlateNo");
        List<? extends Terms.Bucket> data = devices.getBuckets();
        for (org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bks : data) {
            // 输出是为了看看各个值是多少，查询本身输出的数据，就是套牌车了。
            System.out.println("套牌车: " + bks.getKey());
            System.out.println(
                    "PlateNo: "
                            + bks.getKey()
                            + " plateColorDescDistinct: "
                            + ((ParsedCardinality) bks.getAggregations().get("plateColorDescDistinct")).getValue()
                            + " vehicleClassDescDistinct: "
                            + ((ParsedCardinality) bks.getAggregations().get("vehicleClassDescDistinct"))
                            .getValue()
                            + " VehicleBrandDistinct:"
                            + ((ParsedCardinality) bks.getAggregations().get("VehicleBrandDistinct")).getValue()
                            + " SpeedMetrics  "
                    //                            + ((ParsedScriptedMetric)
                    // bks.getAggregations().get("SpeedMetrics")).aggregation()

            );
        }

        client.close();
    }
}
