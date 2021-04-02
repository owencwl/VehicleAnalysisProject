package com.umxwe.common.source.hbase.param;


import com.umxwe.common.param.ParamInfo;
import com.umxwe.common.param.ParamInfoFactory;
import com.umxwe.common.param.WithParams;

import java.util.HashMap;

public interface HbaseSourceParams<T> extends WithParams<T> {

    /**
     * build com.umxwe.common.hbase connect params
     */
    ParamInfo<HashMap> HBASE_CONNNECT_INFO = ParamInfoFactory
            .createParamInfo("HBASE_CONNNECT_INFO", HashMap.class)
            .setDescription("HBASE_CONNNECT_INFO")
            .setHasDefaultValue(new HashMap())
            .build();


}
