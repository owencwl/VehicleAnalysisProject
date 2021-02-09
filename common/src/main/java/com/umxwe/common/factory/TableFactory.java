package com.umxwe.common.factory;

import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.umxwe.common.source.SourceConnector;

import java.io.IOException;

/**
 * @ClassName TableFactory
 * 拿到表对象
 * @Description Todo
 * @Author owen(umxwe))
 * @Data 2020/9/18
 */
public class TableFactory {
    private static final long serialVersionUID = -1071596337076137201L;

    private static final Logger logger = LoggerFactory.getLogger(TableFactory.class);

    private transient SourceConnector conn;
    private boolean isReady = true;

    public TableFactory(SourceConnector conn) {
        this.conn = conn;
    }

    /**
     * get an table object
     * @return
     * @throws IOException
     */
    public Table getTableInstance() throws Exception {
        if (conn == null) {
            if (conn.configure() == null) {
                isReady = false;
                throw new NullPointerException("连接参数为空，重新初始化");
            }
        }
        return isReady ? conn.invoke() : null;
    }
}
