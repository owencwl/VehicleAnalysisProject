package com.umxwe.common.source.elasticsearch;

import com.umxwe.common.param.Params;
import com.umxwe.common.source.SourceConnector;
import com.umxwe.common.source.elasticsearch.param.ElasticSearchSourceParams;
import com.umxwe.common.utils.DataSetConversionUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName ElasticsearchSourceConnector
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2021/1/12
 */
public abstract class ElasticsearchSourceConnector implements SourceConnector {

    private ExecutionEnvironment env;
    private BatchTableEnvironment tEnv;
    private Params elasticSearchSourceParams;
    private ElasticsearchInputFormat esInputFormat;
    private RowTypeInfo rowTypeInfo;
    private String queryConditionStr;


    @Override
    public Params configure() {
        return null;
    }

    public RowTypeInfo setRowTypeInfo(){
        return null;
    }

    @Override
    public void open() {
        env = ExecutionEnvironment.getExecutionEnvironment();
        tEnv = BatchTableEnvironment.create(env);

        elasticSearchSourceParams = this.configure();
        rowTypeInfo=this.setRowTypeInfo();

        queryConditionStr=elasticSearchSourceParams.get(ElasticSearchSourceParams.QUERYCONDITIONSTR);

        esInputFormat = ElasticsearchInputFormat.builder(elasticSearchSourceParams)
                .setQueryConditionStr(queryConditionStr)
                .setRowTypeInfo(rowTypeInfo)
                .build();

    }

    @Override
    public Table invoke() throws Exception {
        this.open();
        DataSource<Row> input = env.createInput(esInputFormat);
        Table table = DataSetConversionUtil.toTable(tEnv, input, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
        DataSetConversionUtil.fromTable(table,tEnv).print();
        return table;
    }

    @Override
    public void close() throws Exception {
        esInputFormat.close();
    }
}
