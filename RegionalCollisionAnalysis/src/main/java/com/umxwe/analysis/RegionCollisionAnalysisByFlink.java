package com.umxwe.analysis;

import com.umxwe.common.param.Params;
import com.umxwe.common.source.elasticsearch.ElasticsearchInputFormat;
import com.umxwe.common.source.elasticsearch.param.ElasticSearchSourceParams;
import com.umxwe.common.utils.DataSetConversionUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * @ClassName RegionCollisionAnalysisByFlink
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class RegionCollisionAnalysisByFlink {

    public static void main(String[] args) throws Exception {

        flinkAPIBatchReadES(null,null);

    }
    public static void flinkAPIBatchReadES(ExecutionEnvironment env, BatchTableEnvironment tEnv) throws Exception {
        if (env == null) {
            env = ExecutionEnvironment.getExecutionEnvironment();
            tEnv = BatchTableEnvironment.create(env);
        }
        String hosts = "10.116.200.21,10.116.200.22,10.116.200.23,10.116.200.25,10.116.200.26";
        String index = "vehicletrajectoryentity";
        int batchSize = 100;
        TimeValue scrollTime = TimeValue.timeValueMinutes(3L);

        Params ESParams = new Params()
                .set(ElasticSearchSourceParams.HTTPHOSTS, hosts)
                .set(ElasticSearchSourceParams.BATCHSIZE, batchSize)
                .set(ElasticSearchSourceParams.INDEX, index)
                .set(ElasticSearchSourceParams.SCROLLTIME, scrollTime);


        /**
         * builder es query params
         */
//        String time1 = "2021-01-01 00:00:00";
//        String time2 = "2021-01-02 00:00:00";
        String time1 = "1610725815509";
        String time2 = "1611827712261";

        RangeQueryBuilder rangequerybuilder = QueryBuilders
                //传入时间，目标格式 2020-01-02T03:17:37.638Z
                .rangeQuery("shotTime")
                .from(time1).to(time2);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(rangequerybuilder);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.STRING, Types.STRING},
                new String[]{"plateNo", "deviceID", "shotTime", "plateColorDesc", "vehicleBrandDesc"});

        /**
         * convert to string type
         */
        String queryConditionStr = boolQueryBuilder.toString();

        ElasticsearchInputFormat esInputFormat = ElasticsearchInputFormat.builder(ESParams)
                .setQueryConditionStr(queryConditionStr)
                .setRowTypeInfo(rowTypeInfo)
                .build();

        DataSource<Row> input = env.createInput(esInputFormat);
        Table table = DataSetConversionUtil.toTable(tEnv, input, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
        DataSet<Row> pvDataSet = DataSetConversionUtil.fromTable(table, tEnv);
        //map bitindex
        pvDataSet.map(new BitIndexBuilderMap())


//                .groupBy(1).reduceGroup(new RichGroupReduceFunction<Tuple3<String, String, Integer>, Object>() {
//            @Override
//            public void reduce(Iterable<Tuple3<String, String, Integer>> values, Collector<Object> out) throws Exception {
//
//            }
//        })
                .print();





//        pvDataSet.print();

    }
}
