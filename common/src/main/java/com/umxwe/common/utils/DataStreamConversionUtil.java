package com.umxwe.common.utils;

/**
 * @ClassName DataStreamConversionUtil
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/17
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * Provide functions of conversions between DataStream and Table.
 */
public class DataStreamConversionUtil {
    /**
     * Convert the given Table to {@link DataStream}<{@link Row}>.
     *
     * @param table     the Table to convert.
     * @return the converted DataStream.
     */
    public static DataStream <Row> fromTable(StreamExecutionEnvironment env,Table table) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        return tEnv.toAppendStream(table, Row.class);
    }

    /**
     * Convert the given DataStream to Table with specified TableSchema.
     *
     * @param data      the DataStream to convert.
     * @param schema    the specified TableSchema.
     * @return the converted Table.
     */
    public static Table toTable(StreamTableEnvironment stenv , DataStream <Row> data, TableSchema schema) {
        return toTable(stenv,data, schema.getFieldNames(), schema.getFieldTypes());
    }



    /**
     * Convert the given DataStream to Table with specified colNames.
     *
     * @param data     the DataStream to convert.
     * @param colNames the specified colNames.
     * @return the converted Table.
     */
    public static Table toTable(StreamTableEnvironment stenv ,DataStream <Row> data, String[] colNames) {
        if (null == colNames || colNames.length == 0) {
            return stenv.fromDataStream(data);
        } else {
            StringBuilder sbd = new StringBuilder();
            sbd.append(colNames[0]);
            for (int i = 1; i < colNames.length; i++) {
                sbd.append(",").append(colNames[i]);
            }
            return stenv.fromDataStream(data, sbd.toString());
        }
    }

    /**
     * Convert the given DataStream to Table with specified colNames and colTypes.
     *
     * @param data     the DataStream to convert.
     * @param colNames the specified colNames.
     * @param colTypes the specified colTypes. This variable is used only when the
     *                 DataSet is produced by a function and Flink cannot determine
     *                 automatically what the produced type is.
     * @return the converted Table.
     */
    public static Table toTable(StreamTableEnvironment stenv , DataStream <Row> data, String[] colNames, TypeInformation <?>[]
            colTypes) {
        try {
            return toTable(stenv,data, colNames);
        } catch (ValidationException ex) {
            if (null == colTypes) {
                throw ex;
            } else {
                DataStream <Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);
                return toTable(stenv,t, colNames);
            }
        }
    }

    /**
     * Adds a type information hint about the colTypes with the Row to the DataStream.
     *
     * @param data     the DataSet to add type information.
     * @param colNames the specified colNames
     * @param colTypes the specified colTypes
     * @return the DataStream with type information hint.
     */
    private static DataStream <Row> getDataSetWithExplicitTypeDefine(
            DataStream <Row> data,
            String[] colNames,
            TypeInformation <?>[] colTypes) {

        DataStream <Row> r = data
                .map(
                        new MapFunction <Row, Row>() {
                            private static final long serialVersionUID = 2265970163148833801L;

                            @Override
                            public Row map(Row t) throws Exception {
                                return t;
                            }
                        }
                )
                .returns(new RowTypeInfo(colTypes, colNames));

        return r;
    }
}