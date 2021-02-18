package com.umxwe.genetedata.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @ClassName RedisMapBuilderReduce
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class RedisMapBuilderReduce implements ReduceFunction<Tuple3<String, String, Object>> {
    protected static final Logger logger = LoggerFactory.getLogger(RedisMapBuilderReduce.class);

    @Override
    public Tuple3<String, String, Object> reduce(Tuple3<String, String, Object> value1, Tuple3<String, String, Object> value2) throws Exception {
        Map<String, String> map1 = (Map<String, String>) value1.f2;
        Map<String, String> map2 = (Map<String, String>) value2.f2;
        map1.putAll(map2);
        logger.info(String.format("reduce map: %s", JSON.toJSONString(map1)));
        return new Tuple3<>(value1.f0, value1.f1, map1);
    }
}