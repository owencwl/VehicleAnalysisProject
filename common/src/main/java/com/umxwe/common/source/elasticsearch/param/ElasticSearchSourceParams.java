package com.umxwe.common.source.elasticsearch.param;

import com.umxwe.common.param.ParamInfo;
import com.umxwe.common.param.ParamInfoFactory;
import com.umxwe.common.param.WithParams;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @ClassName ElasticSearchSourceParam
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/18
 */
public interface ElasticSearchSourceParams<T> extends WithParams<T> {

    /**
     * build httpHosts params
     */
    ParamInfo<String> HTTPHOSTS = ParamInfoFactory
            .createParamInfo("HTTPHOSTS", String.class)
            .setDescription("httpHosts")
            .setHasDefaultValue(null)
            .build();

    /**
     * elasticsearch index
     */
    ParamInfo<String> INDEX = ParamInfoFactory
            .createParamInfo("index", String.class)
            .setDescription("index")
            .setHasDefaultValue("")
            .build();

    /**
     * elasticsearch query batchSize
     */
    ParamInfo<Integer> BATCHSIZE = ParamInfoFactory
            .createParamInfo("batchSize", Integer.class)
            .setDescription("es query batchSize")
            .setHasDefaultValue(10)
            .build();

    /**
     * elasticsearch query scrollTime
     */

    ParamInfo<TimeValue> SCROLLTIME = ParamInfoFactory
            .createParamInfo("scrollTime", TimeValue.class)
            .setDescription("es query scrollTime")
            .setHasDefaultValue(TimeValue.timeValueSeconds(10L))
            .build();


    ParamInfo<RowTypeInfo> ROWTYPEINFO = ParamInfoFactory
            .createParamInfo("RowTypeInfo", RowTypeInfo.class)
            .setDescription("es RowTypeInfo")
            .setHasDefaultValue(null)
            .build();

    ParamInfo<String> QUERYCONDITIONSTR = ParamInfoFactory
            .createParamInfo("RowTypeInfo", String.class)
            .setDescription("es RowTypeInfo")
            .setHasDefaultValue("")
            .build();


}
