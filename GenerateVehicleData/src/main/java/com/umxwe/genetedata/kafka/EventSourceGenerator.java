package com.umxwe.genetedata.kafka;

import com.alibaba.fastjson.JSON;
import com.umxwe.genetedata.entity.VehicleEntity;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static jodd.util.ThreadUtil.sleep;

/**
 * @ClassName EventSourceGenerator
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class EventSourceGenerator  extends RichParallelSourceFunction<String> {
    private static final long serialVersionUID = -3345711794203267205L;
    private long dpv = 1-000-000-000;

    public EventSourceGenerator(long dpv) {
        this.dpv = dpv;
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (dpv > 0) {
            ctx.collect(JSON.toJSONString(new VehicleEntity()));
            sleep(1000);
            dpv--;
        }
    }
    @Override
    public void cancel() {
        dpv = 0;
    }
}
