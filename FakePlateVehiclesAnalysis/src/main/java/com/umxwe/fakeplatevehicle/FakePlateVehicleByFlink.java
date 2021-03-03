package com.umxwe.fakeplatevehicle;

import com.alibaba.fastjson.JSON;
import com.umxwe.genetedata.entity.VehicleEntity;
import com.umxwe.genetedata.entity.VehicleTrajectoryEntity;
import com.umxwe.genetedata.utils.RandomDataUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ClassName FakePlateVehicleByFlink
 * 使用flink读取kafka的数据，按照keyby进行分组，在组内按照时间的先后顺序进行排序，然后再计算速度值，将速度值写入es
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/3/3
 */
public class FakePlateVehicleByFlink {
    private final static Logger logger = LoggerFactory.getLogger(FakePlateVehicleByFlink.class);

    public static void main(String[] args) throws Exception {
        readKafkaStream();
    }

    private static void readKafkaStream() throws Exception {

        String bootstrapServers = "itserver23:6667,itserver22:6667,itserver21:6667";
        String topic_name = "vehicleentity";
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 设置checkpoint的时间
         */
        bsEnv.enableCheckpointing(10000);
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /**
         * 设置为eventtime
         */
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, topic_name);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("group.id", "flink_kafka");
        props.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<String>(topic_name, new SimpleStringSchema(), props);
        DataStream<VehicleEntity> dataStream = bsEnv.addSource(kafkaConsumer).map(new MapFunction<String, VehicleEntity>() {
            @Override
            public VehicleEntity map(String value) throws Exception {
                return JSON.parseObject(value).toJavaObject(VehicleEntity.class);
            }
        });

        /**
         * map成一个新的数据结构
         */
        DataStream<VehicleTrajectoryEntity> dataStreamVehicleTrajectoryEntity = dataStream.map(new MapFunction<VehicleEntity, VehicleTrajectoryEntity>() {
            @Override
            public VehicleTrajectoryEntity map(VehicleEntity value) throws Exception {
                VehicleTrajectoryEntity vehicleTrajectoryEntity = new VehicleTrajectoryEntity();
                vehicleTrajectoryEntity.setPlateClass(value.getPlateClass());
                vehicleTrajectoryEntity.setPlateColor(value.getPlateColor());
                vehicleTrajectoryEntity.setPlateClassDesc(value.getPlateClassDesc());
                vehicleTrajectoryEntity.setPlateColorDesc(value.getPlateColorDesc());
                vehicleTrajectoryEntity.setPlateNo(value.getPlateNo());
                vehicleTrajectoryEntity.setVehicleBrand(value.getVehicleBrand());
                vehicleTrajectoryEntity.setVehicleClass(value.getVehicleClass());
                vehicleTrajectoryEntity.setVehicleColor(value.getVehicleColor());
                vehicleTrajectoryEntity.setVehicleBrandDesc(value.getVehicleBrandDesc());
                vehicleTrajectoryEntity.setVehicleClassDesc(value.getVehicleClassDesc());
                vehicleTrajectoryEntity.setVehicleColorDesc(value.getVehicleColorDesc());

                String date = new SimpleDateFormat("yyyyMMdd").format(vehicleTrajectoryEntity.getShotTime());
                int region = Math.abs((vehicleTrajectoryEntity.getDeviceID() + date).hashCode()) % 5;
                String salt = RandomDataUtil.stringToMD5(Long.toString(vehicleTrajectoryEntity.getShotTime())).substring(0, 5);
                String rowkey = region
                        + date
                        + value.getPlateNo().substring(1, 7)
                        + salt;

                vehicleTrajectoryEntity.setRowKey(rowkey);
                return vehicleTrajectoryEntity;
            }
        });
        /**
         * keyBy 按照plateno进行分组
         */
        KeyedStream<VehicleTrajectoryEntity, String> keyedStream = dataStreamVehicleTrajectoryEntity.keyBy(new KeySelector<VehicleTrajectoryEntity, String>() {
            @Override
            public String getKey(VehicleTrajectoryEntity value) throws Exception {
                return value.getPlateNo();
            }
        });

//        keyedStream.assignTimestampsAndWatermarks(
//                (AssignerWithPeriodicWatermarks<VehicleTrajectoryEntity>) WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofDays(1L)).withTimestampAssigner(new SerializableTimestampAssigner<Object>() {
//                    @Override
//                    public long extractTimestamp(Object element, long recordTimestamp) {
//                        return 0;
//                    }
//                }));

        /**
         * 在keyby流中加入watermark机制，保证实时流的顺序性
         * 其中Time.seconds(5L) 需要根据实际的业务场景来定
         */
        keyedStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<VehicleTrajectoryEntity>(Time.seconds(5L)) {
            /**
             * 用于时间戳抽取器，设置延迟时间，也就是晚到的数据会被丢弃掉
             */
            @Override
            public long extractTimestamp(VehicleTrajectoryEntity element) {
                return element.getShotTime();
            }
        });
        /**
         * sink to es
         */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));

        /**
         * 创建es的索引，索引之前存在则会删除重建
         */
        createESIndex(httpHosts, "carspeedindex");

        /**
         * build es的sink方式
         */
        ElasticsearchSink.Builder<VehicleTrajectoryEntity> esSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<VehicleTrajectoryEntity>() {
                    public IndexRequest createIndexRequest(VehicleTrajectoryEntity element) {
                        /**
                         * 把流中的对象转化为json格式，写入es中
                         */
                        String source = JSON.toJSONString(element);
                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("carspeedindex")
                                .type("carspeedindex")
                                .source(source, XContentType.JSON);
                        return indexRequest;
                    }

                    @Override
                    public void process(VehicleTrajectoryEntity element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });

        /**
         * 按照keyby流进行ProcessFunction处理
         */
        keyedStream.process(new FastCalculateSpeed()).addSink(esSinkBuilder.build());

        bsEnv.execute("FakePlateVehicleByFlink");
    }

    public static void createESIndex(List<HttpHost> httpHosts, String index) throws IOException {
        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
        boolean recreate = true;

        // delete index
        if (recreate && client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            DeleteIndexRequest deleteIndex = new DeleteIndexRequest(index);
            client.indices().delete(deleteIndex, RequestOptions.DEFAULT);

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

                    builder.startObject("deviceID");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("shotTime");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();

                    builder.startObject("location");
                    {
                        builder.field("type", "geo_point");
                    }
                    builder.endObject();

                    builder.startObject("vehicleBrandDesc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("vehicleClassDesc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("vehicleColorDesc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("vehicleBrand");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();


                    builder.startObject("plateClassDesc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("plateColorDesc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("speed");
                    {
                        builder.field("type", "double");
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

    public static class calculateSpeed extends KeyedProcessFunction<String, VehicleTrajectoryEntity, List<VehicleTrajectoryEntity>> {
        private final static Logger logger = LoggerFactory.getLogger(calculateSpeed.class);
        private Map<Long, VehicleTrajectoryEntity> treeMap = new TreeMap<>();

        private MapState<Long, VehicleTrajectoryEntity> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, VehicleTrajectoryEntity> mapStateDescriptor = new MapStateDescriptor("map-state", Long.class, VehicleTrajectoryEntity.class);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(VehicleTrajectoryEntity value, Context context, Collector<List<VehicleTrajectoryEntity>> out) throws Exception {
            logger.info(" context.getCurrentKey():{},time:{}", context.getCurrentKey(), value.getShotTime());
            treeMap.put(value.getShotTime(), value);
//            mapState.put(value.getShotTime(), value);
            /**
             * ontimer定时器，定时完成速度的两两计算
             */

            context.timerService().registerProcessingTimeTimer(5000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<VehicleTrajectoryEntity>> out) throws Exception {

            VehicleTrajectoryEntity preItem = null;
            double speed = 0.0;
//            logger.info("keySet_time:{}", JSON.toJSONString(new ArrayList<Long>(treeMap.keySet())));

//            Iterator<Map.Entry<Long, VehicleTrajectoryEntity>> iterator = mapState.iterator();
//            while (iterator.hasNext()) {
//                Map.Entry<Long, VehicleTrajectoryEntity> item = iterator.next();
//                treeMap.put(item.getKey(), item.getValue());
//            }

            logger.info("treemap_size:{}", treeMap.size());
//            mapState.clear();

            for (Map.Entry<Long, VehicleTrajectoryEntity> item : treeMap.entrySet()
            ) {
                if (preItem != null) {
                    //当前值与上一个值参与计算
                    double distance = arcDistanceCalculate(item.getValue().getShotPlaceLatitude(), item.getValue().getShotPlaceLongitude(), preItem.getShotPlaceLatitude(), preItem.getShotPlaceLongitude(), DistanceUnit.KILOMETERS);
                    double timeInterval = Math.abs(item.getKey() - preItem.getShotTime()) / 1000 * 60 * 60;
                    speed = distance / timeInterval;
                    logger.info("distance:{},timeInterval:{},speedCalculate:{}", distance, timeInterval, speed);
                } else {
                    speed = 0.0;
                }
                // set to speed
                item.getValue().setSpeed(speed);
                preItem = item.getValue();
            }
            out.collect(treeMap.values().stream().collect(Collectors.toList()));
//            treeMap.clear();
        }

        public double arcDistanceCalculate(double srcLat, double srcLon, double dstLat, double dstLon, DistanceUnit unit) {
            return DistanceUnit.convert(arcDistance(srcLat, srcLon, dstLat, dstLon), DistanceUnit.METERS, unit);
        }

        /**
         * Return the distance (in meters) between 2 lat,lon geo points using the haversine method implemented by umxwe
         */
        public double arcDistance(double lat1, double lon1, double lat2, double lon2) {
            return UmxSloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
        }

    }

    /**
     * 在每一个keyby流中进行计算
     */
    public static class FastCalculateSpeed extends ProcessFunction<VehicleTrajectoryEntity, VehicleTrajectoryEntity> {
        private final static Logger logger = LoggerFactory.getLogger(FastCalculateSpeed.class);

        /**
         * 用来保存上一次的记录，使用其中的shottime 和 经纬度坐标
         */
        private ValueState<VehicleTrajectoryEntity> valueState;

        /**
         * 初始化state
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("VehicleTrajectoryEntity", VehicleTrajectoryEntity.class));
        }

        @Override
        public void processElement(VehicleTrajectoryEntity value, Context context, Collector<VehicleTrajectoryEntity> out) throws Exception {
            logger.info(" context.timestamp:{},time:{},value.getPlateNo:{}", context.timestamp(), value.getShotTime(), value.getPlateNo());
            double speed = 0.0;
            /**
             * 流中的第一个数据是没有速度值，默认为0.0
             */
            if (valueState.value() == null) {
                speed = 0.0;
            } else {
                /**
                 * 1、使用arc的方式计算两个经纬度坐标直接的距离，转换为km
                 * 2、时间间隔转换为hour
                 */
                double distance = arcDistanceCalculate(value.getShotPlaceLatitude(), value.getShotPlaceLongitude(), valueState.value().getShotPlaceLatitude(), valueState.value().getShotPlaceLongitude(), DistanceUnit.KILOMETERS);
                double timeInterval = Math.abs(value.getShotTime() - valueState.value().getShotTime()) / 1000 * 60 * 60;
                speed = distance / timeInterval;
                logger.info("distance:{},timeInterval:{},speedCalculate:{} km/h", distance, timeInterval, speed);
            }
            value.setSpeed(speed);
            /**
             * 当前值设置为state，用于下一次计算
             */
            valueState.update(value);
            /**
             * 将结果返回出去
             */
            out.collect(value);
        }

        /**
         * 经纬度距离计算，可以单位转换
         *
         * @param srcLat
         * @param srcLon
         * @param dstLat
         * @param dstLon
         * @param unit
         * @return
         */
        public double arcDistanceCalculate(double srcLat, double srcLon, double dstLat, double dstLon, DistanceUnit unit) {
            return DistanceUnit.convert(arcDistance(srcLat, srcLon, dstLat, dstLon), DistanceUnit.METERS, unit);
        }

        /**
         * Return the distance (in meters) between 2 lat,lon geo points using the haversine method implemented by umxwe
         */
        public double arcDistance(double lat1, double lon1, double lat2, double lon2) {
            return UmxSloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
        }

    }


}
