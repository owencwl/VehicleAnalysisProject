package com.umxwe.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.calcite.shaded.com.google.common.hash.Hashing;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName BitIndexBuilderMap
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class BitIndexBuilderMap extends RichMapFunction<Row, Tuple3<String, String, Integer>> {

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
        config.useSingleServer().setAddress("redis://10.116.200.24:6379")
                .setPassword("youmei");
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
    public Tuple3<String, String, Integer> map(Row in) throws Exception {
        String plateNo = in.getField(0).toString();
        //分片
        int shardingNum = Math.abs(hash.hashBytes(plateNo.getBytes()).asInt()) % REDIS_CLUSTER_SHARDING_MODE;
        String mapKey = USER_BITINDEX_SHARDING_KEY + shardingNum;
        RMap<String, String> rMap = redissonClient.getMap(mapKey);
        // 如果为空,生成一个bitIndex
        String bitIndexStr = rMap.get(plateNo);
        if (StringUtils.isEmpty(bitIndexStr)) {
            LOG.info("userId[{}]的bitIndex为空, 开始生成bitIndex", plateNo);
            RLock lock = redissonClient.getLock(GLOBAL_COUNTER_LOCKER_KEY);
            try {
                lock.tryLock(60, TimeUnit.SECONDS);
                // 再get一次
                bitIndexStr = rMap.get(plateNo);
                if (StringUtils.isEmpty(bitIndexStr)) {
                    RAtomicLong atomic = redissonClient.getAtomicLong(GLOBAL_COUNTER_KEY);
                    bitIndexStr = String.valueOf(atomic.incrementAndGet());
                }
                rMap.put(plateNo, bitIndexStr);
            } finally {
                lock.unlock();
            }
            LOG.info("plateNo[{}]的bitIndex生成结束, bitIndex: {}", plateNo, bitIndexStr);
        }

        LOG.info("plateNo[{}] map to bitIndex: {}", plateNo, bitIndexStr);
        return new Tuple3<>(plateNo, "map", Integer.valueOf(bitIndexStr));
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