import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

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
                new WordCount(1, "Hello", 1),
                new WordCount(1, "World", 10),
                new WordCount(2, "Hello", 1)};
        env.fromElements(data)
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                })
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                        logger.info("value1:{} value2:{}", JSON.toJSONString(value1), JSON.toJSONString(value2));
                        return new WordCount(1, value1.getWord(),value1.getFrequency()+value2.getFrequency());
                    }
                })
                .addSink(new SinkFunction<WordCount>() {
                    @Override
                    public void invoke(WordCount value, Context context) throws Exception {
                        logger.info("WordCount:{}", JSON.toJSONString(value));
                    }
                });
        env.execute("testMax");
    }


}
