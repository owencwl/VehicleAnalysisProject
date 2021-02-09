package com.umxwe.common.source.elasticsearch;

/**
 * @ClassName ElasticsearchInputFormat
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/17
 */
import com.umxwe.common.param.Params;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 获取es的所有的数据,提供格式转换
 */
public class ElasticsearchInputFormat extends ElasticsearchInputFormatBase<Row> {
    private final static Logger logger = LoggerFactory.getLogger(ElasticsearchInputFormat.class);

    private static final long serialVersionUID = 1L;

//    private DeserializationSchema<Row> deserializationSchema;

    public ElasticsearchInputFormat(Params ESParams, String queryConditionStr, RowTypeInfo rowTypeInfo) {
        super(ESParams,queryConditionStr,rowTypeInfo);
    }

    /**
     * 将searchHit 转化为json，如需转化为其他的可以重写该方法
     * @param hit SearchHit
     * @return String  json
     */
//    @Override
//    public String transform(SearchHit hit) {
//        return hit.getSourceAsString();
//    }

    /**
     * transfer to org.apache.flink.types.Row
     * @param hit SearchHit
     * @param reuse
     * @return
     * @throws IOException
     */
    @Override
    public Row transform(SearchHit hit,Row reuse) throws IOException {
        for (Map.Entry<String, Object> entry : hit.getSourceAsMap().entrySet()) {
                Integer p = position.get(entry.getKey());
                if (p == null) {
                    throw new IOException("unknown field " + entry.getKey());
                }
                reuse.setField(p, entry.getValue());
        }
//        Row row = null;
//        try {
//            row = deserializationSchema.deserialize(hit.getSourceAsString().getBytes());
//        } catch (IOException e) {
//            logger.error("Deserialize search hit failed: " + e.getMessage());
//        }
        return reuse;
    }



    public static Builder builder(Params ESParams) {
        return new Builder(ESParams);
    }
    public static class Builder {

        private Params ESParams;
        private String queryConditionStr;
        private RowTypeInfo rowTypeInfo;

        public Builder(Params ESParams) {
            this.ESParams = ESParams;
        }

        public Builder setQueryConditionStr(String queryConditionStr) {
            this.queryConditionStr=queryConditionStr;
            return this;
        }
        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
            return this;
        }

        /**
         * 创建ElasticsearchInputFormat实例
         * @return ElasticsearchInputFormat
         */
        public ElasticsearchInputFormat build() {
            return new ElasticsearchInputFormat(this.ESParams,this.queryConditionStr,this.rowTypeInfo);
        }
    }

}