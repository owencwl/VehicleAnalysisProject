package com.umxwe.common.source;

import com.umxwe.common.param.Params;
import org.apache.flink.table.api.Table;

public interface SourceConnector {

    /**
     * add com.umxwe.common.source connect params
     *
     * @return
     */
     Params configure();


    /**
     * open connector
     */
    void open() throws Exception;

    /**
     * read datasource
     */

    Table invoke() throws Exception;

    /**
     * close connector
     */
    void close() throws Exception;

}
