package com.umxwe.genetedata;

import com.alibaba.fastjson.JSON;
import com.umxwe.genetedata.entity.VehicleEntity;
import com.umxwe.genetedata.entity.VehicleTrajectoryEntity;
import com.umxwe.genetedata.threadpool.ThreadPoolManager;
import com.umxwe.genetedata.utils.ConcurrentDataProducerutil;
import com.umxwe.genetedata.utils.RandomDataUtil;
import com.umxwe.common.hbase.utils.MyHBase;
import com.umxwe.common.hbase.utils.MyHTable;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.umxwe.common.utils.TimeUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @ClassName Bootstrap
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
public class Bootstrap {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

    public static void main(String[] args) throws Exception {
//        generateVehicleData();
        generateVehicleTrajectoryData();
//        getVehicleTrajectoryData();

//        generateSingleData();

//        System.out.println(JSON.toJSONString(DateProcessUtils.handleRangeDate("2020-01-01 08:12:13","2020-01-01 08:00:00")));


//        generateVehicleDataByFlink();




    }

    private static void generateSingleData() throws IOException {
        MyHBase myHBase = new MyHBase();
        MyHTable myHTable = myHBase.getTable(VehicleTrajectoryEntity.class);
//        myHBase.createMetaTable(null,VehicleEntity.class);//手工进行创建元数据表
        MyHTable myHTable1 = myHBase.getTable(VehicleEntity.class);

        VehicleTrajectoryEntity vehicleTrajectoryEntity=new VehicleTrajectoryEntity();
        vehicleTrajectoryEntity.setRowKey("120210102TYEYRQ800838");
        myHTable.put(vehicleTrajectoryEntity);
    }

    private static void getVehicleTrajectoryData() throws Exception {
        TimeUtils.now();
        MyHBase myHBase = new MyHBase();
        MyHTable myHTable = myHBase.getTable(VehicleTrajectoryEntity.class);
//        List<VehicleTrajectoryEntity> results = myHTable.scanByMultFilter((float) 0.005, VehicleTrajectoryEntity.class);
//        logger.info(JSON.toJSONString(results.get(0)));
//        System.out.println(JSON.toJSONString(results.size()) + "耗时：" + TimeUtils.timeInterval());
//        logger.info(JSON.toJSONString(myHTable.get("5519da20210104CYWIQI",VehicleTrajectoryEntity.class)));

//        TimeUtils.now();
//        List<VehicleTrajectoryEntity> results1 = myHTable.scan("320210101WEOQOR477e6","420210130YRYROT786ae", VehicleTrajectoryEntity.class);
//        System.out.println(JSON.toJSONString(results1.size()) + "耗时：" + TimeUtils.timeInterval());

        TimeUtils.now();
        String deviceid="4312000000119000072";
        String date="20210101";
        int region=Math.abs((deviceid+date).hashCode())%5;

        List<VehicleTrajectoryEntity> results2 = myHTable.scanByRegexRow(region+date+"*", VehicleTrajectoryEntity.class);
        logger.info(region+date+"*" +"##########"+JSON.toJSONString(results2.subList(0,5)));
        System.out.println(JSON.toJSONString(results2.size()) + "耗时：" + TimeUtils.timeInterval());



    }


    public static void generateVehicleData() throws Exception {
        MyHBase myHBase = new MyHBase();
        MyHTable myHTable = myHBase.getTable(VehicleEntity.class);
        /**
         * total size: 200 * 10000 = 200W
         */
        for (int i = 0; i < 200; i++) {
            List<VehicleEntity> list = ConcurrentDataProducerutil.producerListDataByMutlThread(10000, VehicleEntity.class);
            logger.info("开始插入");
            TimeUtils.now();

            myHTable.put(list);
            logger.info(String.format("插入第%s批次，每一批次为10000，耗时：%s ms", i + 1, TimeUtils.timeInterval()));

        }
    }
    public static void generateVehicleDataByFlink() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setDouble("taskmanager.memory.network.fraction", 0.1);
        configuration.setString("taskmanager.memory.network.min", "1024m");
        configuration.setString("taskmanager.memory.network.max", "1024m");
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironment(4,configuration);
        senv.setParallelism(4);

        DataStream<VehicleEntity> producer=senv.addSource(new RichParallelSourceFunction<VehicleEntity>() {
            private long dpv = 2-000-000;

            @Override
            public void run(SourceContext<VehicleEntity> ctx) throws Exception {
                while (dpv > 0) {
                    ctx.collect(new VehicleEntity());
                    dpv--;
                }
            }
            @Override
            public void cancel() {
                dpv=0;
            }
        });

        producer.print();
        senv.execute();

    }

    public static void generateVehicleTrajectoryData() throws Exception {

        //flink 读取hbase 1w
        TimeUtils.now();
        MyHBase myHBase = new MyHBase();
        MyHTable myHTable = myHBase.getTable(VehicleEntity.class);
        //0.005 * 200W = 1w左右
        List<VehicleEntity> results = myHTable.scanByRandom((float) 0.005, VehicleEntity.class);
        System.out.println(JSON.toJSONString(results.size()) + "耗时：" + TimeUtils.timeInterval());


        // 随机设置卡口数据 10~20  部分100

//        insertListDataByMutlThread(results);

        for (int i = 0; i < 10; i++) {
            TimeUtils.now();
            List<VehicleTrajectoryEntity> vehicleTrajectoryLists = generateData(results);

            //insert hbase
//            System.out.println(JSON.toJSONString(vehicleTrajectoryLists.subList(0,10)));
//            MyHTable vehicleTrajectoryEntityHTable = myHBase.getTable(VehicleTrajectoryEntity.class);
//            vehicleTrajectoryEntityHTable.put(vehicleTrajectoryLists);
//            System.out.println(String.format("第%s批次大小:%s,写入hbase耗时：%s ms", i + 1, vehicleTrajectoryLists.size(), TimeUtils.timeInterval()));

            //insert es
            generateDataToES(vehicleTrajectoryLists);

        }
        System.out.println(String.format("总耗时：%s ms", TimeUtils.timeInterval()));

        //flink 写入hbase

    }


    public static List<VehicleTrajectoryEntity> generateData(List<VehicleEntity> lists) throws Exception {
        TimeUtils.now();
        int dealSize = 200;

        int preThreadNum = (int) (lists.size() / dealSize) + 1;
        ExecutorService executorService = ThreadPoolManager.getInstance().getExecutorServer(preThreadNum);

        //结果集
        List<VehicleTrajectoryEntity> list = new ArrayList<VehicleTrajectoryEntity>(lists.size() * 10);
        List<Future<List<VehicleTrajectoryEntity>>> futureList = new ArrayList<Future<List<VehicleTrajectoryEntity>>>(preThreadNum);

        int index = 0;

        //分配
        for (int i = 0; i <= preThreadNum; i++, index += dealSize) {
            int start = index;
            if (start >= lists.size()) break;
            int end = start + dealSize;
            end = end > lists.size() ? lists.size() : end;
//            logger.info(start + "  " + end);

            futureList.add(executorService.submit(new CallableTask(lists.subList(start, end))));
        }
        for (Future<List<VehicleTrajectoryEntity>> future : futureList) {
            while (true) {
                if (future.isDone() && !future.isCancelled()) {
                    List<VehicleTrajectoryEntity> futureResultList = future.get();//获取结果
                    list.addAll(futureResultList);
                    break;
                } else {
                    //每次轮询休息1毫秒（CPU纳秒级），避免CPU高速轮循耗空CPU
                    Thread.sleep(1);
                }
            }
        }
        logger.info("多线程生成原始数据总耗时： " + TimeUtils.timeInterval() + " ms");

        return list;
    }

    private static class CallableTask implements Callable<List<VehicleTrajectoryEntity>> {
        private List<VehicleEntity> lists;

        public CallableTask(List<VehicleEntity> lists) {
            this.lists = lists;
        }

        @Override
        public List<VehicleTrajectoryEntity> call() throws Exception {
            /**
             * VehicleEntity 和 VehicleTrajectoryEntity 合并
             */
            List<VehicleTrajectoryEntity> vehicleTrajectoryLists = new ArrayList<>();

            for (VehicleEntity vehicleEntity : this.lists
            ) {
                for (int i = 0; i < 10; i++) {
                    VehicleTrajectoryEntity vehicleTrajectoryEntity = new VehicleTrajectoryEntity();
                    vehicleTrajectoryEntity.setPlateClass(vehicleEntity.getPlateClass());
                    vehicleTrajectoryEntity.setPlateColor(vehicleEntity.getPlateColor());
                    vehicleTrajectoryEntity.setPlateClassDesc(vehicleEntity.getPlateClassDesc());
                    vehicleTrajectoryEntity.setPlateColorDesc(vehicleEntity.getPlateColorDesc());
                    vehicleTrajectoryEntity.setPlateNo(vehicleEntity.getPlateNo());
                    vehicleTrajectoryEntity.setVehicleBrand(vehicleEntity.getVehicleBrand());
                    vehicleTrajectoryEntity.setVehicleClass(vehicleEntity.getVehicleClass());
                    vehicleTrajectoryEntity.setVehicleColor(vehicleEntity.getVehicleColor());
                    vehicleTrajectoryEntity.setVehicleBrandDesc(vehicleEntity.getVehicleBrandDesc());
                    vehicleTrajectoryEntity.setVehicleClassDesc(vehicleEntity.getVehicleClassDesc());
                    vehicleTrajectoryEntity.setVehicleColorDesc(vehicleEntity.getVehicleColorDesc());

//                    String timestamp = vehicleTrajectoryEntity.getShotTime().substring(0, 4) +
//                            vehicleTrajectoryEntity.getShotTime().substring(5, 7) +
//                            vehicleTrajectoryEntity.getShotTime().substring(8, 10);
//                    String rowkey = RandomDataUtil.stringToMD5(vehicleEntity.getPlateNo()).substring(0, 6) + timestamp + vehicleEntity.getPlateNo().substring(1, 7);


                    String date=new SimpleDateFormat("yyyyMMdd").format(vehicleTrajectoryEntity.getShotTime());
                    int region=Math.abs((vehicleTrajectoryEntity.getDeviceID()+date).hashCode())%5;
//                    int salt=Math.abs(new Long(vehicleTrajectoryEntity.getShotTime()).hashCode())%1000000;
                    String salt= RandomDataUtil.stringToMD5(Long.toString(vehicleTrajectoryEntity.getShotTime())).substring(0, 5);
                    String rowkey =region
                            +date
                            +vehicleEntity.getPlateNo().substring(1, 7)
                            +salt;

                    vehicleTrajectoryEntity.setRowKey(rowkey);
                    vehicleTrajectoryLists.add(vehicleTrajectoryEntity);
                }

            }
            return vehicleTrajectoryLists;
        }
    }


    public static void generateDataToES( List<VehicleTrajectoryEntity> vehicleTrajectoryLists) throws InterruptedException, IOException {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.116.200.21", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.22", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.23", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.24", 9200, "http"));
        httpHosts.add(new HttpHost("10.116.200.25", 9200, "http"));


        //创建低级客户端,提供ip,端口,设置超时重试时间
        RestClientBuilder restClient = RestClient.builder((HttpHost[]) httpHosts.toArray(new HttpHost[httpHosts.size()]));
        //创建高级客户端,传入低级客户端
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClient);
        createIndex(restHighLevelClient,"vehicletrajectoryentity");

        batchInsert(restHighLevelClient,"vehicletrajectoryentity",vehicleTrajectoryLists);

    }
    public static void batchInsert(RestHighLevelClient client, String indexName , List<VehicleTrajectoryEntity> vehicleTrajectoryLists) throws IOException {
        // TODO Auto-generated method stub
        BulkRequest request = new BulkRequest();
        for (VehicleTrajectoryEntity item : vehicleTrajectoryLists) {
            Map<String,Object> m  = item.toMap();
            IndexRequest indexRequest= new IndexRequest(indexName, indexName, String.valueOf(item.getRowKey())).source(m);
            request.add(indexRequest);
        }
        client.bulk(request, RequestOptions.DEFAULT);
        System.out.println("批量插入完成");
    }
    public static void createIndex(RestHighLevelClient restHighLevelClient, String index) throws IOException {

        /**
         * 为字段carrier_name添加索引
         * curl -X PUT "10.116.200.22:9200/eventdata/_mapping?pretty" -H 'Content-Type: application/json' -d'
         * {
         *   "properties": {
         *     "carrier_name": {
         *       "type":     "text",
         *       "fielddata": true
         *     }
         *   }
         * }
         * '
         */

        //若索引存在，先删除
        if (restHighLevelClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            DeleteIndexRequest deleteIndex = new DeleteIndexRequest(index);
            restHighLevelClient.indices().delete(deleteIndex, RequestOptions.DEFAULT);
        }
        //重新建造索引
        // mapping config and put
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.startObject("properties");
//            {
//                builder.startObject("carrier_name");
//                {
//                    builder.field("type", "text");
//                }
//                builder.endObject();
//            }
//            builder.endObject();
//        }
//        builder.endObject();
//        // create index
        CreateIndexRequest createIndex = new CreateIndexRequest(index);
//        createIndex.mapping(index, builder);
//

        /**
         * 使用map的形式建造索引
         *
         * Map<String, Object> message = new HashMap<>();
         * message.put("type", "text");
         * Map<String, Object> properties = new HashMap<>();
         * properties.put("carrier_name", message);
         * Map<String, Object> mapping = new HashMap<>();
         * mapping.put("properties", properties);
         * request.mapping(mapping);
         */


        createIndex.settings(
                Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 0));
        restHighLevelClient.indices().create(createIndex, RequestOptions.DEFAULT);
    }

}
