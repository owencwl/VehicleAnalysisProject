package com.umxwe.genetedata.flink;

import com.alibaba.fastjson.JSON;
import com.umxwe.common.factory.TableFactory;
import com.umxwe.common.hbase.utils.MyHBase;
import com.umxwe.common.hbase.utils.MyHTable;
import com.umxwe.common.param.Params;
import com.umxwe.common.source.hbase.HbaseSourceConnector;
import com.umxwe.common.source.hbase.param.HbaseSourceParams;
import com.umxwe.common.utils.DataStreamConversionUtil;
import com.umxwe.genetedata.entity.VehicleEntity;
import com.umxwe.genetedata.entity.VehicleTrajectoryEntity;
import com.umxwe.genetedata.utils.RandomDataUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
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
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName PlateNoMapRedisByFlink
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class PlateNoMapRedisByFlink {
    protected static final Logger logger = LoggerFactory.getLogger(PlateNoMapRedisByFlink.class);

    public static void main(String[] args) throws Exception {
//        plateNoMapBitindex();
//        plateNoMapBitindexByFlinksql();
        plateNoMapBitindexByStream();
//        testredisconnect();
    }

    /**
     * 使用flink sql 的方式读取hbase 的数据
     *
     * @throws Exception
     */
    private static void plateNoMapBitindexByFlinksql() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        Configuration hbaseClientConf = new Configuration();
        hbaseClientConf.set("hbase.zookeeper.quorum", "itserver21:2181,itserver22:2181,itserver23:2181");
        hbaseClientConf.set("zookeeper.znode.parent", "/hbase-unsecure");

        HBaseTableSource hbaseStreamTableSource = new HBaseTableSource(hbaseClientConf, "VehicleEntity");
        hbaseStreamTableSource.setRowKey("rowkey", String.class);
        hbaseStreamTableSource.addColumn("fm", "c10", String.class);
        stenv.registerTableSource("VehicleEntity", hbaseStreamTableSource);
//        Table table = stenv.sqlQuery("select t.rowkey,t.c10 from VehicleEntity as t where t.rowkey like '020210101%'");
        Table table = stenv.sqlQuery("select t.rowkey,t.c10 from VehicleEntity as t ");
        DataStream<Row> dataStream = DataStreamConversionUtil.fromTable(senv, table);
        dataStream.print();
        senv.execute("flink sql read hbase");
    }

    /**
     * 使用flink-hbase 原始api的方式进行读取hbase ,目前只能全表扫描，根据条件过滤还需调试
     *
     * @throws Exception
     */
    private static void plateNoMapBitindex() throws Exception {
        //VehicleEntity:c10 ;VehicleTrajectoryEntity :c6

        Table hbaseTable = new TableFactory(new HbaseSourceConnector() {
            @Override
            public Params configure() {
                HashMap<String, String> tableProperties = new HashMap<>();
                tableProperties.put("connector.type", "hbase");
                tableProperties.put("connector.version", "1.4.3");
                tableProperties.put("connector.property-version", "1");
                tableProperties.put("connector.table-name", "VehicleEntity");
                tableProperties.put("connector.zookeeper.quorum", "itserver21,itserver22,itserver23");
                tableProperties.put("connector.zookeeper.znode.parent", "/hbase-unsecure");
                tableProperties.put("connector.write.buffer-flush.max-size", "10mb");
                tableProperties.put("connector.write.buffer-flush.max-rows", "1000");
                tableProperties.put("connector.write.buffer-flush.interval", "10s");

                Params hbaseSourceParams = new Params()
                        .set(HbaseSourceParams.HBASE_CONNNECT_INFO, tableProperties);
                return hbaseSourceParams;
            }

            @Override
            public TableSchema setTableSchema() {
                TableSchema schema =
                        TableSchema.builder()
                                .field(
                                        "fm",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("c10", DataTypes.STRING())))
                                .field("rowkey", DataTypes.STRING())
                                .build();
                return schema;
            }
        }).getTableInstance();

    }


    public static void plateNoMapBitindexByStream() throws Exception {
        String bootstrapServers = "itserver23:6667,itserver22:6667,itserver21:6667";
        String topic_name = "vehicleentity";

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        conf.setInteger(RestOptions.PORT, 9096);
//        conf.setString(RestOptions.BIND_PORT, "9093-9094");
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(10000);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, topic_name);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("group.id", "flink_kafka");
        props.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<String>(topic_name, new SimpleStringSchema(), props);
        SingleOutputStreamOperator dataStream = bsEnv.addSource(kafkaConsumer).map(new MapFunction<String, VehicleEntity>() {
            @Override
            public VehicleEntity map(String value) throws Exception {
                return JSON.parseObject(value).toJavaObject(VehicleEntity.class);
            }
        });

//        dataStream.countWindowAll(10000).apply(new AllWindowFunction<VehicleEntity, List<VehicleEntity>, GlobalWindow>() {
//            @Override
//            public void apply(GlobalWindow window, Iterable<VehicleEntity> message, Collector<List<VehicleEntity>> out) throws Exception {
//                List<VehicleEntity> putList = new ArrayList<>();
//                for (VehicleEntity value : message) {
//                    putList.add(value);
//                }
//                out.collect(putList);
//            }
//            //to hbase
//        }).addSink(new HBaseSinkFunction());
//
//        dataStream.map(new RedisMapBuilderFunction())
//                .keyBy(new KeySelector<Tuple3<String, String, Object>, String>() {
//                    @Override
//                    public String getKey(Tuple3<String, String, Object> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .reduce(new RedisMapBuilderReduce())
//                .addSink(new RedisSinkFunction());


        /**
         * sink VehicleTrajectoryEntity  to hbase and es
         */

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "elasticsearch");
        //该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "1");
        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));
//        httpHosts.add(new HttpHost("10.116.200.26", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));

        //重新运行需要注释这行code，不然会删除索引
        createESIndex(httpHosts, "carindex");

        ElasticsearchSink.Builder<VehicleTrajectoryEntity> esSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<VehicleTrajectoryEntity>() {
                    public IndexRequest createIndexRequest(VehicleTrajectoryEntity element) {
                        //把流中的对象转化为json格式，写入es中
                        String source = JSON.toJSONString(element);
                        return Requests.indexRequest()
                                .index("carindex")
                                .type("carindex")
                                .source(source, XContentType.JSON);
                    }

                    @Override
                    public void process(VehicleTrajectoryEntity element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });
        dataStream.map(new MapFunction<VehicleEntity, VehicleTrajectoryEntity>() {
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
//                    int salt=Math.abs(new Long(vehicleTrajectoryEntity.getShotTime()).hashCode())%1000000;
                String salt = RandomDataUtil.stringToMD5(Long.toString(vehicleTrajectoryEntity.getShotTime())).substring(0, 5);
                String rowkey = region
                        + date
                        + value.getPlateNo().substring(1, 7)
                        + salt;

                vehicleTrajectoryEntity.setRowKey(rowkey);
                return vehicleTrajectoryEntity;
            }
        }).addSink(esSinkBuilder.build());

        bsEnv.execute("KafkaSinkToHbaseAndRedis");

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

    /**
     * This class implements an HBaseSinkFunction for HBase.
     */
    public static class HBaseSinkFunction extends RichSinkFunction<List<VehicleEntity>> {
        MyHBase myHBase;
        MyHTable myHTable;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            myHBase = new MyHBase();
            myHTable = myHBase.getTable(VehicleEntity.class);
        }

        @Override
        public void invoke(List<VehicleEntity> putList, Context context) throws Exception {
            myHTable.put(putList);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    public static class RedisSinkFunction extends RichSinkFunction<Tuple3<String, String, Object>> {
        private RedissonClient redissonClient;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Config config = new Config();
            config.setCodec(new StringCodec());
            config.useSingleServer().setAddress("redis://10.116.200.24:6379")
                    .setPassword("youmei");
            redissonClient = Redisson.create(config);
        }

        @Override
        public void close() throws Exception {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }

        @Override
        public void invoke(Tuple3<String, String, Object> record, Context context) throws Exception {
//            logger.info(String.format("write redis: %s", JSON.toJSONString(record)));
            String key = record.f0;
            RKeys rKeys = redissonClient.getKeys();
            rKeys.delete(key);
            String keyType = record.f1;
            if ("STRING".equalsIgnoreCase(keyType)) {
                String value = (String) record.f2;
                RBucket<String> rBucket = redissonClient.getBucket(key);
                rBucket.set(value);
            } else if ("MAP".equalsIgnoreCase(keyType)) {
                Map<String, String> map = (Map<String, String>) record.f2;
                RMap<String, String> rMap = redissonClient.getMap(key);
                rMap.putAll(map);
            } else if ("ATOMICLONG".equalsIgnoreCase(keyType)) {
                long l = (long) record.f2;
                RAtomicLong atomic = redissonClient.getAtomicLong(key);
                atomic.set(l);
            }
        }
    }

    public static void testredisconnect() {
        RedissonClient redissonClient;
        Config config = new Config();
        config.setCodec(new StringCodec());
        config.useSingleServer().setAddress("redis://10.116.200.24:6379")
                .setPassword("youmei");
        redissonClient = Redisson.create(config);
        Map<String, String> map = new HashMap<>();
        map.put("owen", "chen");
        RMap<String, String> rMap = redissonClient.getMap("owen");
        rMap.putAll(map);
    }

    /**
     * 将string 转换为bitindex 写入redis
     * @throws Exception
     */
//    public static void demo() throws Exception {
//        // main方法
////        final ExecutionEnvironment env = buildExecutionEnv();
//        final ExecutionEnvironment env = null;
//        //如果没有找到好的方法保证id单调递增，就设置一个并行度
//        env.setParallelism(1);
//
//        TextInputFormat input = new TextInputFormat(new Path(""));
//        input.setCharsetName("UTF-8");
//        DataSet<String> source = env.createInput(input).filter(e -> !e.startsWith("user_id")).map(
//                new MapFunction<String, String>() {
//                    @Override
//                    public String map(String value) throws Exception {
//                        String[] arr = StringUtils.split(value, ",");
//                        return arr[0];
//                    }
//                })
//                .distinct();
//        source
//                .map(new RedisMapBuilderFunction())
//                .groupBy(0)
//                .reduce(new RedisMapBuilderReduce())CountDistinctFunction
//                .output(new RedissonOutputFormat());
//
//        long counter = source.count();
//        env.fromElements(counter).map(new MapFunction<Long, Tuple3<String, String, Object>>() {
//            @Override
//            public Tuple3<String, String, Object> map(Long value) throws Exception {
//                return new Tuple3<>("FLINK:GLOBAL:BITINDEX", "ATOMICLONG", value);
//            }
//        }).output(new RedissonOutputFormat());
//    }
}
