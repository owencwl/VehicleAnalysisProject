package com.umxwe.genetedata.flink;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName RedissonOutputFormat
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
//输出 到redis
public class RedissonOutputFormat extends RichOutputFormat<Tuple3<String, String, Object>> {

    private RedissonClient redissonClient;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Config config = new Config();
        config.setCodec(new StringCodec());
        config.useClusterServers().addNodeAddress(getRedissonNodes("itserver24:6379"))
                .setPassword("youmei").setSlaveConnectionMinimumIdleSize(1)
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
     * k,type,value
     *
     * @param record
     * @throws IOException
     */
    @Override
    public void writeRecord(Tuple3<String, String, Object> record) throws IOException {
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

    @Override
    public void close() throws IOException {
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