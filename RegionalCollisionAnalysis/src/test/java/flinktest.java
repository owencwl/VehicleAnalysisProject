import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName flinktest
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class flinktest {
    protected static final Logger logger = LoggerFactory.getLogger(flinktest.class);

    @Test
    public void testMax() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WordCount[] data = new WordCount[]{
                new WordCount(1, "Hello", 1),
                new WordCount(1, "Hello", 2),
                new WordCount(1, "World", 10),
                new WordCount(2, "Hello", 3)};
        env.fromElements(data)
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                })
                .process(new KeyedProcessFunction<String, WordCount, WordCount>() {
                    private ValueState<Map<String,RoaringBitmap>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Types.MAP(Types.STRING,Types.GENERIC(RoaringBitmap.class))));
                    }

                    @Override
                    public void processElement(WordCount value, Context ctx, Collector<WordCount> out) throws Exception {
                        logger.info("processElement:{}, key:{}", JSON.toJSONString(value),ctx.getCurrentKey());

                        Map<String,RoaringBitmap> current = state.value();
                        if (current == null) {
                            current = new HashMap<>();
//                            current.f0 = new RoaringBitmap();
                        }
                        if(current.containsKey(ctx.getCurrentKey())){
                            current.get(ctx.getCurrentKey()).add(value.getFrequency());
                        }else {
                            current.put(ctx.getCurrentKey(),RoaringBitmap.bitmapOf(value.getFrequency()));
                        }


                        long processingTime = ctx.timerService().currentProcessingTime();
                        if(current.isEmpty() ||  current.get(ctx.getCurrentKey()).getLongCardinality() + 10000 <= processingTime) {
                            // write the state back
                            state.update(current);
                            ctx.timerService().registerProcessingTimeTimer(current.get(ctx.getCurrentKey()).getLongCardinality()+ 10000);
                        } else {
                            state.update(current);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WordCount> out) throws Exception {
                        String key = ctx.getCurrentKey();
                        Map<String,RoaringBitmap> result = state.value();
                        logger.info("result:{}", JSON.toJSONString(result));
//                        RoaringBitmap temp=new RoaringBitmap();
//                        for (Map.Entry<String,RoaringBitmap> item:result.entrySet()
//                             ) {
////                            temp=RoaringBitmap.and(item.getValue(),);
//                        }

                        //new Tuple2<>(key, result.f0.getLongCardinality())
//                        out.collect(new WordCount("999",key,result.f0.getLongCardinality()));

                    }
                })
//                .reduce(new ReduceFunction<WordCount>() {
//                    @Override
//                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
//                        logger.info("value1:{} value2:{}", JSON.toJSONString(value1), JSON.toJSONString(value2));
//                        return new WordCount(1, value1.getWord(),value1.getFrequency()+value2.getFrequency());
//                    }
//                })
                .addSink(new SinkFunction<WordCount>() {
                    @Override
                    public void invoke(WordCount value, Context context) throws Exception {
                        logger.info("WordCount:{}", JSON.toJSONString(value));
                    }
                });
        env.execute("testMax");
    }


}
