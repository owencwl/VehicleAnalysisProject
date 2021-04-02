package com.umxwe.genetedata.flink;

import com.umxwe.genetedata.entity.VehicleEntity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.hash.HashFunction;
import org.apache.flink.calcite.shaded.com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RedisMapBuilderFunction
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
// 注意分区逻辑和key要和stream的保持一致
public class RedisMapBuilderFunction implements MapFunction<VehicleEntity, Tuple3<String, String, Object>> {
    protected static final Logger logger = LoggerFactory.getLogger(RedisMapBuilderFunction.class);

    private static final String USER_BITINDEX_SHARDING_KEY = "FLINK:BITINDEX:SHARDING:";
    private static final Integer REDIS_CLUSTER_SHARDING_MODE = 100;
    private HashFunction hash = Hashing.crc32();
    private Integer counter = 0;

    @Override
    public Tuple3<String, String, Object> map(VehicleEntity vehicleEntity) throws Exception {
        counter++;
        String plateNo = vehicleEntity.getPlateNo();
        int shardingNum = Math.abs(hash.hashBytes(plateNo.getBytes()).asInt()) % REDIS_CLUSTER_SHARDING_MODE;
        String key = USER_BITINDEX_SHARDING_KEY + shardingNum;
        Map<String, String> map = new HashMap<>();
        map.put(plateNo, String.valueOf(counter));
//        logger.info(String.format("Tuple3,key:%s map:%s ",key, JSON.toJSONString(map)));
        return new Tuple3<>(key, "MAP", map);
    }
}