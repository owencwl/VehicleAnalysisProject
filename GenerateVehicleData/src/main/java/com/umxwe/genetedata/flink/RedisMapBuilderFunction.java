package com.umxwe.genetedata.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.calcite.shaded.com.google.common.hash.Hashing;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RedisMapBuilderFunction
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
// 注意分区逻辑和key要和stream的保持一致
public class RedisMapBuilderFunction implements MapFunction<Row, Tuple3<String, String, Object>> {

    private static final String USER_BITINDEX_SHARDING_KEY = "FLINK:BITINDEX:SHARDING:";

    private static final Integer REDIS_CLUSTER_SHARDING_MODE = 100;

    private HashFunction hash = Hashing.crc32();
    private Integer counter = 0;

    @Override
    public Tuple3<String, String, Object> map(Row plateRow) throws Exception {
        counter ++;
        String plateNo=plateRow.getField(0).toString();
        System.out.println("userId.getField(0).toString():"+plateRow.getField(0).toString());

        int shardingNum = Math.abs(hash.hashBytes(plateRow.getField(0).toString().getBytes()).asInt()) % REDIS_CLUSTER_SHARDING_MODE;
        String key = USER_BITINDEX_SHARDING_KEY + shardingNum;
        Map<String, String> map = new HashMap<>();
        map.put(plateNo, String.valueOf(counter));
        return new Tuple3<>(key, "MAP", map);
    }
}