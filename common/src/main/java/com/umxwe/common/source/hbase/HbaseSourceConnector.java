package com.umxwe.common.source.hbase;

import com.umxwe.common.param.Params;
import com.umxwe.common.source.SourceConnector;
import com.umxwe.common.source.hbase.param.HbaseSourceParams;
import com.umxwe.common.utils.DataStreamConversionUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_NODE_PARENT;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * @ClassName HBaseInputFormat
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2021/1/12
 */
public abstract class HbaseSourceConnector implements SourceConnector {

    private StreamExecutionEnvironment senv;
    private StreamTableEnvironment stenv;
    private HBaseTableSource hbaseStreamTableSource;
    private DataStream<Row> hbaseDataStream;

    @Override
    public Params configure() {
        return null;
    }

    private DescriptorProperties createDescriptor(TableSchema tableSchema) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putTableSchema(SCHEMA, tableSchema);
        descriptorProperties.putProperties(this.configure().get(HbaseSourceParams.HBASE_CONNNECT_INFO));
        return descriptorProperties;
    }

    @Override
    public void open() throws Exception {
        senv = StreamExecutionEnvironment.createLocalEnvironment();
        stenv = StreamTableEnvironment.create(senv);
        DescriptorProperties descriptorProperties = createDescriptor(this.setTableSchema());
        String hTableName = descriptorProperties.getString(CONNECTOR_TABLE_NAME);

        Configuration hbaseClientConf = HBaseConfiguration.create();
        String hbaseZk = descriptorProperties.getString(CONNECTOR_ZK_QUORUM);
        hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZk);
        descriptorProperties
                .getOptionalString(CONNECTOR_ZK_NODE_PARENT)
                .ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));
        HBaseTableSchema hbaseSchema = validateTableSchema(tableSchema);

        hbaseStreamTableSource = new HBaseTableSource(hbaseClientConf, hTableName, hbaseSchema, null);
        /**
         *  * HBaseTableSource hSrc = new HBaseTableSource(conf, "hTable");
         *  * hSrc.setRowKey("rowkey", String.class);
         *  * hSrc.addColumn("fam1", "col1", byte[].class);
         *  * hSrc.addColumn("fam1", "col2", Integer.class);
         *  * hSrc.addColumn("fam2", "col1", String.class);
         *  *
         *  * tableEnv.registerTableSourceInternal("hTable", hSrc);
         *  * Table res = tableEnv.sqlQuery(
         *  *   "SELECT t.fam2.col1, SUM(t.fam1.col2) FROM hTable AS t " +
         *  *   "WHERE t.rowkey LIKE 'flink%' GROUP BY t.fam2.col1");
         *  * }
         */
    }

    public TableSchema setTableSchema() {
        return null;
    }

    @Override
    public Table invoke() throws Exception {
        this.open();

        hbaseDataStream = hbaseStreamTableSource.getDataStream(senv);
        //stream need sink and  execute function,etc.
        hbaseDataStream.print();
        senv.execute();
        return DataStreamConversionUtil.toTable(stenv, hbaseDataStream, hbaseStreamTableSource.getTableSchema());

        //transformat to Row
//        DataStream<Row> rows = hbaseDataStream.map(new MapFunction<Row, Row>() {
//            @Override
//            public Row map(Row value) throws Exception {
//                System.out.println(value.toString());
//                return null;
//            }
//        });
    }

    @Override
    public void close() throws Exception {

    }

    private HBaseTableSchema validateTableSchema(TableSchema schema) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();


        String[] fieldNames = schema.getFieldNames();
        TypeInformation[] fieldTypes = schema.getFieldTypes();


        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
            TypeInformation<?> type = fieldTypes[i];
            if (type instanceof RowTypeInfo) {
                RowTypeInfo familyType = (RowTypeInfo) type;
                String[] qualifierNames = familyType.getFieldNames();
                TypeInformation[] qualifierTypes = familyType.getFieldTypes();
                for (int j = 0; j < familyType.getArity(); j++) {
                    // HBase connector doesn't support LocalDateTime
                    // use Timestamp as conversion class for now.
                    Class clazz = qualifierTypes[j].getTypeClass();
                    if (LocalDateTime.class.equals(clazz)) {
                        clazz = Timestamp.class;
                    } else if (LocalDate.class.equals(clazz)) {
                        clazz = Date.class;
                    } else if (LocalTime.class.equals(clazz)) {
                        clazz = Time.class;
                    }
                    hbaseSchema.addColumn(name, qualifierNames[j], clazz);
                }
            } else {
                hbaseSchema.setRowKey(name, type.getTypeClass());
            }
        }
        return hbaseSchema;
    }
}