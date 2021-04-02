package com.umxwe.common.utils;

/**
 * @ClassName DataSetConversionUtil
 * @Description Todo
 * @Author owen(umxwe))
 * @Date 2020/12/16
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Provide functions of conversions between DataSet and Table.
 */
public class DataSetConversionUtil {
    /**
     * Convert the given Table to {@link DataSet}<{@link Row}>.
     *
     * @param table the Table to convert.
     * @return the converted DataSet.
     */
    public static DataSet<Row> fromTable(Table table, BatchTableEnvironment tEnv) {
        return tEnv.toDataSet(table, Row.class);
    }

    /**
     * Convert the given DataSet into a Table with specified TableSchema.
     *
     * @param data   the DataSet to convert.
     * @param schema the specified TableSchema.
     * @return the converted Table.
     */
    public static Table toTable(BatchTableEnvironment tEnv, DataSet<Row> data, TableSchema schema) {
        return toTable(tEnv, data, schema.getFieldNames(), schema.getFieldTypes());
    }


    /**
     * Convert the given DataSet into a Table with specified colNames and colTypes.
     *
     * @param data     the DataSet to convert.
     * @param colNames the specified colNames.
     * @param colTypes the specified colTypes. This variable is used only when the DataSet is
     *                 produced by a function and Flink cannot determine automatically what the
     *                 produced type is.
     * @return the converted Table.
     */
    public static Table toTable(BatchTableEnvironment tEnv, DataSet<Row> data, String[] colNames, TypeInformation<?>[] colTypes) {
        try {
            // Try to add row type information for the dataset to be converted.
            // In most case, this keeps us from the rolling back logic in the catch block,
            // which adds an unnecessary map function just in order to add row type information.
            try {
                if (data instanceof SingleInputUdfOperator) {
                    ((SingleInputUdfOperator) data).returns(new RowTypeInfo(colTypes, colNames));
                } else if (data instanceof TwoInputUdfOperator) {
                    ((TwoInputUdfOperator) data).returns(new RowTypeInfo(colTypes, colNames));
                }
            } catch (IllegalStateException ex) {
                //pass
            }

            return toTable(tEnv, data, colNames);
        } catch (ValidationException ex) {
            if (null == colTypes) {
                throw ex;
            } else {
                DataSet<Row> t = getDataSetWithExplicitTypeDefine(data, colNames, colTypes);
                return toTable(tEnv, t, colNames);
            }
        }
    }

    /**
     * Convert the given DataSet into a Table with specified colNames.
     *
     * @param data     the DataSet to convert.
     * @param colNames the specified colNames.
     * @return the converted Table.
     */
    public static Table toTable(BatchTableEnvironment tEnv, DataSet<Row> data, String[] colNames) {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        if (null == colNames || colNames.length == 0) {
            return tEnv.fromDataSet(data);
        } else {
            StringBuilder sbd = new StringBuilder();
            sbd.append(colNames[0]);
            for (int i = 1; i < colNames.length; i++) {
                sbd.append(",").append(colNames[i]);
            }
            return tEnv.fromDataSet(data, sbd.toString());
        }
    }

    /**
     * Adds a type information hint about the colTypes with the Row to the DataSet.
     *
     * @param data     the DataSet to add type information.
     * @param colNames the specified colNames
     * @param colTypes the specified colTypes
     * @return the DataSet with type information hint.
     */
    private static DataSet<Row> getDataSetWithExplicitTypeDefine(
            DataSet<Row> data,
            String[] colNames,
            TypeInformation<?>[] colTypes) {

        DataSet<Row> r = data
                .map(
                        new MapFunction<Row, Row>() {
                            private static final long serialVersionUID = 4962235907261477641L;

                            @Override
                            public Row map(Row t) throws Exception {
                                return t;
                            }
                        }
                )
                .returns(new RowTypeInfo(colTypes, colNames));

        return r;
    }

    /**
     * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. It will
     * ignore the case of the tableCols.
     *
     * @param tableCols a string array among which to find the targetCol.
     * @param targetCol the targetCol to find.
     * @return the index of the targetCol, if not found, returns -1.
     */
    public static int findColIndex(String[] tableCols, String targetCol) {
        Preconditions.checkNotNull(targetCol, "targetCol is null!");
        for (int i = 0; i < tableCols.length; i++) {
            if (targetCol.equalsIgnoreCase(tableCols[i])) {
                return i;
            }
        }
        return -1;
    }
}