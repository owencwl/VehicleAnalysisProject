package com.umxwe.genetedata.flink;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.calcite.shaded.com.google.common.hash.Hashing;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName BitmapDistinctByFlink
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class BitmapDistinctByFlink {
    public static void main(String[] args) {

        // main方法
//        final ExecutionEnvironment env = buildExecutionEnv();
        final ExecutionEnvironment env = null;

        //如果没有找到好的方法保证id单调递增，就设置一个并行度
        env.setParallelism(1);

        TextInputFormat input = new TextInputFormat(new Path(""));
        input.setCharsetName("UTF-8");
        DataSet<String> source = env.createInput(input).filter(e -> !e.startsWith("user_id")).map(
                new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        String[] arr = StringUtils.split(value, ",");
                        return arr[0];
                    }
                })
                .distinct();

//        env.addSource(com.umxwe.common.source).map(new MapFunction<String, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> map(String value) throws Exception {
//                String[] arr = StringUtils.split(value, ",");
//                return new Tuple2<>(arr[0], arr[1]);
//            }
//        })
//                .keyBy(0) //根据userId分组
//                .map(new BitIndexBuilderMap()) //构建bitindex
//                .keyBy(1) //统计页面下的访问人数
//                .process(new CountDistinctFunction())
//                .print();


    }


    public static class BitIndexBuilderMap extends RichMapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

        private static final Logger LOG = LoggerFactory.getLogger(BitIndexBuilderMap.class);

        private static final String GLOBAL_COUNTER_KEY = "FLINK:GLOBAL:BITINDEX";

        private static final String GLOBAL_COUNTER_LOCKER_KEY = "FLINK:GLOBAL:BITINDEX:LOCK";

        private static final String USER_BITINDEX_SHARDING_KEY = "FLINK:BITINDEX:SHARDING:";

        /**
         * 把用户id分散到redis的100个map中，防止单个map的无限扩大，也能够充分利用redis cluster的分片功能
         */
        private static final Integer REDIS_CLUSTER_SHARDING_MODE = 100;

        private HashFunction hash = Hashing.crc32();

        private RedissonClient redissonClient;

        @Override
        public void open(Configuration parameters) throws Exception {
//    ParameterTool globalPara = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Config config = new Config();
            config.setCodec(new StringCodec());
            config.useClusterServers().addNodeAddress(getRedissonNodes("redis1:8080,redis2:8080,redis3:8080"))
                    .setPassword("xxxx").setSlaveConnectionMinimumIdleSize(1)
                    .setMasterConnectionPoolSize(2)
                    .setMasterConnectionMinimumIdleSize(1)
                    .setSlaveConnectionPoolSize(2)
                    .setSlaveConnectionMinimumIdleSize(1)
                    .setConnectTimeout(10000)
                    .setTimeout(10000)
                    .setIdleConnectionTimeout(10000);
            redissonClient = Redisson.create(config);
        }

        /**
         * 把userId递增化,在redis中建立一个id映射关系
         *
         * @param in
         * @return
         * @throws Exception
         */
        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, String> in) throws Exception {
            String userId = in.f0;
            //分片
            int shardingNum = Math.abs(hash.hashBytes(userId.getBytes()).asInt()) % REDIS_CLUSTER_SHARDING_MODE;
            String mapKey = USER_BITINDEX_SHARDING_KEY + shardingNum;
            RMap<String, String> rMap = redissonClient.getMap(mapKey);
            // 如果为空,生成一个bitIndex
            String bitIndexStr = rMap.get(userId);
            if (StringUtils.isEmpty(bitIndexStr)) {
                LOG.info("userId[{}]的bitIndex为空, 开始生成bitIndex", userId);
                RLock lock = redissonClient.getLock(GLOBAL_COUNTER_LOCKER_KEY);
                try {
                    lock.tryLock(60, TimeUnit.SECONDS);
                    // 再get一次
                    bitIndexStr = rMap.get(userId);
                    if (StringUtils.isEmpty(bitIndexStr)) {
                        RAtomicLong atomic = redissonClient.getAtomicLong(GLOBAL_COUNTER_KEY);
                        bitIndexStr = String.valueOf(atomic.incrementAndGet());
                    }
                    rMap.put(userId, bitIndexStr);
                } finally {
                    lock.unlock();
                }
                LOG.info("userId[{}]的bitIndex生成结束, bitIndex: {}", userId, bitIndexStr);
            }
            return new Tuple3<>(in.f0, in.f1, Integer.valueOf(bitIndexStr));
        }

        @Override
        public void close() throws Exception {
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
        }

        private String[] getRedissonNodes(String hosts) {
            List<String> nodes = new ArrayList<>();
            if (hosts == null || hosts.isEmpty()) {
                return null;
            }
            String nodexPrefix = "redis://";
            String[] arr = StringUtils.split(hosts, ",");
            for (String host : arr) {
                nodes.add(nodexPrefix + host);
            }
            return nodes.toArray(new String[nodes.size()]);
        }
    }

    public static class CountDistinctFunction extends KeyedProcessFunction<Tuple, Tuple3<String, String, Integer>, Tuple2<String, Long>> {

        private static final Logger LOG = LoggerFactory.getLogger(CountDistinctFunction.class);

        private ValueState<Tuple2<RoaringBitmap, Long>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Types.TUPLE(Types.GENERIC(RoaringBitmap.class), Types.LONG)));
        }

        /**
         * @param in  Tuple3<String, String, Integer> : Tuple3<shizc, www.baidu.com, 0001>
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Tuple3<String, String, Integer> in, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // retrieve the current count
            Tuple2<RoaringBitmap, Long> current = state.value();
            if (current == null) {
                current = new Tuple2<>();
                current.f0 = new RoaringBitmap();
            }
            current.f0.add(in.f2);

            long processingTime = ctx.timerService().currentProcessingTime();
            if (current.f1 == null || current.f1 + 10000 <= processingTime) {
                current.f1 = processingTime;
                // write the state back
                state.update(current);
                ctx.timerService().registerProcessingTimeTimer(current.f1 + 10000);
            } else {
                state.update(current);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            Tuple1<String> key = (Tuple1<String>) ctx.getCurrentKey();
            Tuple2<RoaringBitmap, Long> result = state.value();

            result.f0.runOptimize();
            out.collect(new Tuple2<>(key.f0, result.f0.getLongCardinality()));
        }
    }
    /**
     * shizc,www.baidu..com
     * shizc,www.baidu.com
     * shizc1,www.baidu.com
     * shizc2,www.baidu.com
     * shizc,www.baidu..com
     * shizc,www.baidu..com
     * shizc,www.baidu..com
     * shizc,www.hahaha.com
     * shizc,www.hahaha.com
     * shizc1,www.hahaha.com
     * shizc2,www.hahaha.com
     *
     * 输出 ：
     * (www.baidu.com,4)
     * (www.hahaha.com,3)
     */
}
