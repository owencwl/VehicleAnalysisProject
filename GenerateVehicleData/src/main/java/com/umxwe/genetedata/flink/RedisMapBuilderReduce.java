package com.umxwe.genetedata.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

/**
 * @ClassName RedisMapBuilderReduce
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class RedisMapBuilderReduce implements ReduceFunction<Tuple3<String, String, Object>> {
    @Override
    public Tuple3<String, String, Object> reduce(Tuple3<String, String, Object> value1, Tuple3<String, String, Object> value2) throws Exception {
        Map<String, String> map1 = (Map<String, String>) value1.f2;
        Map<String, String> map2 = (Map<String, String>) value2.f2;
        map1.putAll(map2);
        return new Tuple3<>(value1.f0, value1.f1, map1);
    }
}