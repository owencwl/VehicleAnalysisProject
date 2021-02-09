package com.umxwe.common.hbase.utils;

/**
 * @ClassName MyHTable
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class MyHTable<T> {
    MyHBase myHBase;
    private Map<String, String> ncmap = new HashMap<>();
    private Map<String, CType> ntmap = new HashMap<>();
    private String rowkeyname = null;
    private String tablename;
    private boolean autoFlush = true;

    private SimplePool<Table> tablePool = new SimplePool<>();

    MyHTable(MyHBase myHBase, String tabname, String defs, boolean autoflush) {
        this.tablename = tabname;
        this.myHBase = myHBase;
        this.autoFlush = autoflush;
        String[] fds = defs.split(",");
        for (String fd : fds) {
            String[] ss = fd.split(":");
            ncmap.put(ss[1], ss[0]);
            ntmap.put(ss[1], CType.getCType(ss[2]));
            if ("rowkey".equalsIgnoreCase(ss[1]))
                rowkeyname = ss[1];
            else if (rowkeyname == null && "id".equalsIgnoreCase(ss[1])) {
                rowkeyname = ss[1];
            }
        }
    }

    private Map<String, Object> mapObject(T object) {
        Map<String, Object> kvmap = new HashMap<>();
        Class t = object.getClass();
        for (String kname : ncmap.keySet()) {
            try {
                Method mth = t.getDeclaredMethod("get" + kname, null);
                Object value = mth.invoke(object, null);
                if (value != null) {
                    kvmap.put(kname, value);
                }
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        return kvmap;
    }

    public void put(T object) throws IOException {
        Put put = this.buildPut(object);
        Table table = this.getTable();
        table.put(put);
//        table.checkAndPut(put.getRow(), Bytes.toBytes("fd"),Bytes.toBytes("c3"), null, put);
        table.close();
    }

    private Put buildPut(T object) {
        Map<String, Object> kvmap = this.mapObject(object);
        Put put = new Put(Bytes.toBytes(kvmap.get(this.rowkeyname).toString()));
        for (String k : kvmap.keySet()) {
            if (ncmap.containsKey(k)) {
                byte[] bs = null;
                Object val = kvmap.get(k);
                CType ct = ntmap.get(k);
                bs = CType.toBytes(ct, val);
                put.addColumn(Bytes.toBytes(MyHBase.DATA_FAMILY), Bytes.toBytes(ncmap.get(k)), bs);
            }
        }
        return put;
    }

    public T get(String rowkey, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        Table table = getTable();
        Get g1 = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(g1);
        if (result == null || result.isEmpty())
            return null;
        T obj = this.result2Object(result, t);

        table.close();
        return obj;
    }

    public List<T> get(List<String> rowkeys, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<Get> gets = new ArrayList<>(rowkeys.size());
        for (int i = 0; i < rowkeys.size(); i++) {
            gets.add(new Get(Bytes.toBytes(rowkeys.get(i))));
        }
        List<T> rlist = Collections.emptyList();
        Table table = getTable();
        Result results[] = table.get(gets);
        if (results != null && results.length > 0) {
            rlist = new ArrayList<>(results.length);
            for (int i = 0; i < results.length; i++) {
                rlist.add(result2Object(results[i], t));
            }
        }
        return rlist;
    }

    private T result2Object(Result result, Class<T> t) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        T obj = t.newInstance();
        Method[] methods = t.getDeclaredMethods();
        for (String key : ncmap.keySet()) {
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(MyHBase.DATA_FAMILY), Bytes.toBytes(ncmap.get(key)));
            if (cell != null && cell.getValueLength() > 0) {
                Optional<Method> m = Arrays.stream(methods).filter(mm -> mm.getName().equalsIgnoreCase("set" + key) && mm.getReturnType().equals(void.class)).findFirst();
                if (m.isPresent()) {
                    Method mt = m.get();
                    CType ct = ntmap.get(key);
                    byte[] bs = CellUtil.cloneValue(cell);
                    Object arg = CType.toObject(ct, bs);
                    mt.invoke(obj, new Object[]{arg});
                }
            }
        }
        return obj;
    }

    public void put(T[] objs) throws IOException {
        Put[] puts = new Put[objs.length];
        for (int i = 0; i < objs.length; i++) {
            puts[i] = buildPut(objs[i]);
        }

        Table table = getTable();
        table.put(Arrays.asList(puts));
        table.close();
    }

    public void put(List<T> objs) throws IOException {
        List<Put> puts = new ArrayList<>(objs.size());
        for (int i = 0; i < objs.size(); i++) {
            puts.add(buildPut(objs.get(i)));
        }
        Table table = getTable();
        table.put(puts);
        table.close();
    }

    public void delete(String rowkey) throws IOException {
        Table table = getTable();
        Delete del = new Delete(Bytes.toBytes(rowkey));
        del.addFamily(Bytes.toBytes(MyHBase.DATA_FAMILY));
        table.delete(del);
        table.close();
    }

    public void delete(List<String> rowkeys) throws IOException {
        Table table = getTable();
        List<Delete> list = new ArrayList<>();
        rowkeys.forEach(m -> {
            list.add(new Delete(Bytes.toBytes(m)));
        });
        table.delete(list);
        table.close();
    }

    public int delete(String startKey, String endKey) throws IOException {
        Table table = getTable();

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        scan.setFilter(new KeyOnlyFilter());

        List<Delete> dels = new ArrayList<>();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            Delete d = new Delete(r.getRow());
            dels.add(d);
        }
        table.delete(dels);
        table.close();
        return dels.size();
    }

    public List<T> get(String[] rowkeys, Class<T> t) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        return this.get(Arrays.asList(rowkeys), t);
    }

    public List<T> scan(String startKey, String endKey, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        Table table = getTable();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        scan.addFamily(Bytes.toBytes(MyHBase.DATA_FAMILY));
        ResultScanner resultScanner = table.getScanner(scan);
        Result res = null;
        while ((res = resultScanner.next()) != null) {
            if (!res.isEmpty())
                list.add(result2Object(res, t));
        }
        resultScanner.close();
        table.close();
        return list;
    }

    /**
     *
     * @param chance 表示有一定的机会取得部分数据，例如：chance=0.0005，200W总数据*0.0005=1000条左右的数据
     * @param t
     * @return
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public List<T> scanByRandom(float chance, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        Table table = getTable();
        Scan scan = new Scan();
        Filter filter = new RandomRowFilter(chance); //
        scan.setFilter(filter);
        scan.addFamily(Bytes.toBytes(MyHBase.DATA_FAMILY));
        ResultScanner resultScanner = table.getScanner(scan);
        Result res = null;
        while ((res = resultScanner.next()) != null) {
            if (!res.isEmpty())
                list.add(result2Object(res, t));
        }
        resultScanner.close();
        table.close();
        return list;
    }

    /**
     * 多个过滤器
     * @param chance
     * @param t
     * @return
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public List<T> scanByMultFilter(float chance, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        Table table = getTable();
        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes("320210101WEOQOR477e6"));
//        scan.setStopRow(Bytes.toBytes("420210130YRYROT786ae"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = new RandomRowFilter(chance); //
        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new  BinaryComparator(Bytes.toBytes("c8")));//只获取车牌号列
        filterList.addFilter(Arrays.asList(filter1,filter2));
        scan.setFilter(filterList);

        scan.addFamily(Bytes.toBytes(MyHBase.DATA_FAMILY));
        ResultScanner resultScanner = table.getScanner(scan);
        Result res = null;
        while ((res = resultScanner.next()) != null) {
            if (!res.isEmpty())
                list.add(result2Object(res, t));
        }
        resultScanner.close();
        table.close();
        return list;
    }


    public List<T> scanByRegexRow(String regexRowkeyStr, Class<T> t) throws IOException, IllegalAccessException, InstantiationException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        Table table = getTable();
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexRowkeyStr));
        Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new  BinaryComparator(Bytes.toBytes("c8")));//只获取车牌号列
        filterList.addFilter(Arrays.asList(filter1,filter2));
        scan.setFilter(filterList);
        scan.addFamily(Bytes.toBytes(MyHBase.DATA_FAMILY));
        ResultScanner resultScanner = table.getScanner(scan);
        Result res = null;
        while ((res = resultScanner.next()) != null) {
            if (!res.isEmpty())
                list.add(result2Object(res, t));
        }
        resultScanner.close();
        table.close();
        return list;

    }

    private Table getTable() throws IOException {
        if (tablePool.count() == 0) {
            try (Connection connection = this.myHBase.getConnection()) {
                Table table = connection.getTable(TableName.valueOf(this.tablename), this.myHBase.getExecutorService());
//                ((HTable) table).setAutoFlushTo(autoFlush);
                tablePool.addSource(table);
            }
        }
        try {
            return tablePool.getSource();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IOException("tablePool.getSource failed");
        }
    }



}