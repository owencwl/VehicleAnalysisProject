package com.umxwe.genetedata.flink;

import com.umxwe.common.factory.TableFactory;
import com.umxwe.common.hbase.utils.MyHBase;
import com.umxwe.common.utils.DataSetConversionUtil;
import com.umxwe.common.utils.DataStreamConversionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import com.umxwe.common.param.Params;
import com.umxwe.common.source.hbase.HbaseSourceConnector;
import com.umxwe.common.source.hbase.param.HbaseSourceParams;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName PlateNoMapRedisByFlink
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/9
 */
public class PlateNoMapRedisByFlink {
    public static void main(String[] args) throws Exception {
//        plateNoMapBitindex();
        plateNoMapBitindexByFlinksql();
    }

    /**
     * 使用flink sql 的方式读取hbase 的数据
     * @throws Exception
     */
    private static void plateNoMapBitindexByFlinksql() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        Configuration hbaseClientConf =new Configuration ();
        hbaseClientConf.set("hbase.zookeeper.quorum","itserver21:2181,itserver22:2181,itserver23:2181");
        hbaseClientConf.set("zookeeper.znode.parent","/hbase-unsecure");

        HBaseTableSource hbaseStreamTableSource = new HBaseTableSource(hbaseClientConf, "VehicleEntity");
        hbaseStreamTableSource.setRowKey("rowkey", String.class);
        hbaseStreamTableSource.addColumn("fm", "c10", String.class);
        stenv.registerTableSource("VehicleEntity", hbaseStreamTableSource);
//        Table table = stenv.sqlQuery("select t.rowkey,t.c10 from VehicleEntity as t where t.rowkey like '020210101%'");
        Table table = stenv.sqlQuery("select t.rowkey,t.c10 from VehicleEntity as t ");
        DataStream<Row> dataStream = DataStreamConversionUtil.fromTable(senv, table);
        dataStream.print();
        senv.execute("flink sql read hbase");
    }

    /**
     * 使用flink-hbase 原始api的方式进行读取hbase ,目前只能全表扫描，根据条件过滤还需调试
     * @throws Exception
     */
    private static void plateNoMapBitindex() throws Exception {
        //VehicleEntity:c10 ;VehicleTrajectoryEntity :c6

        Table hbaseTable = new TableFactory(new HbaseSourceConnector() {
            @Override
            public Params configure() {
                HashMap<String, String> tableProperties = new HashMap<>();
                tableProperties.put("connector.type", "hbase");
                tableProperties.put("connector.version", "1.4.3");
                tableProperties.put("connector.property-version", "1");
                tableProperties.put("connector.table-name", "VehicleEntity");
                tableProperties.put("connector.zookeeper.quorum", "itserver21,itserver22,itserver23");
                tableProperties.put("connector.zookeeper.znode.parent", "/hbase-unsecure");
                tableProperties.put("connector.write.buffer-flush.max-size", "10mb");
                tableProperties.put("connector.write.buffer-flush.max-rows", "1000");
                tableProperties.put("connector.write.buffer-flush.interval", "10s");

                Params hbaseSourceParams = new Params()
                        .set(HbaseSourceParams.HBASE_CONNNECT_INFO, tableProperties);
                return hbaseSourceParams;
            }

            @Override
            public TableSchema setTableSchema() {
//                TableSchema schema =
//                        TableSchema.builder()
//                                .field(
//                                        "uvpv",
//                                        DataTypes.ROW(
//                                                DataTypes.FIELD("uv", DataTypes.STRING()),
//                                                DataTypes.FIELD("pv", DataTypes.STRING())))
//                                .field("rowkey", DataTypes.BIGINT())
//                                .build();
                TableSchema schema =
                        TableSchema.builder()
                                .field(
                                        "fm",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("c10", DataTypes.STRING())))
                                .field("rowkey", DataTypes.STRING())
                                .build();
                return schema;
            }
        }).getTableInstance();


//        DataSetConversionUtil.fromTable(hbaseTable,tEnv)
//                .map(new RedisMapBuilderFunction())
//                .groupBy(0)
//                .reduce(new RedisMapBuilderReduce())
//                .output(new RedissonOutputFormat());

//        DataStreamConversionUtil.fromTable(null,hbaseTable).map(new RedisMapBuilderFunction()).keyBy(0).reduce(new RedisMapBuilderReduce()).addSink();



    }

    /**
     * 将string 转换为bitindex 写入redis
     * @throws Exception
     */
//    public static void demo() throws Exception {
//        // main方法
////        final ExecutionEnvironment env = buildExecutionEnv();
//        final ExecutionEnvironment env = null;
//        //如果没有找到好的方法保证id单调递增，就设置一个并行度
//        env.setParallelism(1);
//
//        TextInputFormat input = new TextInputFormat(new Path(""));
//        input.setCharsetName("UTF-8");
//        DataSet<String> source = env.createInput(input).filter(e -> !e.startsWith("user_id")).map(
//                new MapFunction<String, String>() {
//                    @Override
//                    public String map(String value) throws Exception {
//                        String[] arr = StringUtils.split(value, ",");
//                        return arr[0];
//                    }
//                })
//                .distinct();
//        source
//                .map(new RedisMapBuilderFunction())
//                .groupBy(0)
//                .reduce(new RedisMapBuilderReduce())
//                .output(new RedissonOutputFormat());
//
//        long counter = source.count();
//        env.fromElements(counter).map(new MapFunction<Long, Tuple3<String, String, Object>>() {
//            @Override
//            public Tuple3<String, String, Object> map(Long value) throws Exception {
//                return new Tuple3<>("FLINK:GLOBAL:BITINDEX", "ATOMICLONG", value);
//            }
//        }).output(new RedissonOutputFormat());
//    }
}
