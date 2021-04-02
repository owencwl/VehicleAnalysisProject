package com.umxwe.common.hbase.utils;

/**
 * @ClassName MyHBase
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * MyHBase类用户创建，删除表，以及管理Connection对象。
 */
public class MyHBase {
    private static final Logger logger = LoggerFactory.getLogger(MyHBase.class);

    public static final String META_TABLE = "default:info";
    public static final String META_FAMILY = "fm";
    public static final String META_COLUMN = "cm";
    public static final String DATA_FAMILY = "fm";

    private Configuration configuration;
    private SimplePool<Connection> connectionSimplePool = new SimplePool<>();
    static AtomicInteger atomicInteger = new AtomicInteger();
    private ExecutorService executorService = Executors.newFixedThreadPool(3, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("mhb_thread" + atomicInteger.getAndIncrement());
            return t;
        }
    });

    public MyHBase() {
        if (this.configuration == null) {
            this.configuration = HBaseConfiguration.create();
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * table definitions map 从元数据Info表中获取得到表的数据字段定义
     */
    private Map<String, String> tdsmap = new HashMap<>();

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = HBaseConfiguration.create(configuration);
    }

    /**
     * 创建MyHTable对象
     *
     * @param t
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T> MyHTable<T> getTable(Class<T> t) throws IOException {
        return this.getTable(null, t);
    }

    public <T> MyHTable<T> getTable(String namespace, Class<T> t) throws IOException {
        return this.getTable(namespace, t, true);
    }

    public <T> MyHTable<T> getTable(String namespace, Class<T> t, boolean autoflush) throws IOException {
        String tds = tdsmap.get(tableName(namespace, t));
        if (tds == null) {
            if (!this.tableExist(namespace, t)) {
                createTable(namespace, t);
            }
            loadTableMeta();
            tds = tdsmap.get(tableName(namespace, t));
            if (tds == null) {
                throw new IOException("there is no table " + tableName(t) + ", please create firstly");
            }
        }
        MyHTable<T> ta = new MyHTable<T>(this, tableName(namespace, t), tds, autoflush);
        return ta;
    }

    /**
     * 获取myhtable对象， 当表不存在时，自动创建表
     *
     * @param namespace    表的命名空间
     * @param t            POJO类
     * @param autoflush    写入数据是是否自动autoflush, 默认为true，为false时，速度会更快
     * @param compressName 压缩算法
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T> MyHTable<T> getTable(String namespace, Class<T> t, boolean autoflush, String compressName) throws IOException {
        String tds = tdsmap.get(tableName(namespace, t));
        if (tds == null) {
            if (!this.tableExist(namespace, t)) {
                if (compressName != null && compressName.trim().length() > 0) {
                    createTable(namespace, t, Compression.getCompressionAlgorithmByName(compressName.trim().toLowerCase()));
                } else {
                    createTable(namespace, t);
                }
            }
            loadTableMeta();
            tds = tdsmap.get(tableName(namespace, t));
            if (tds == null) {
                throw new IOException("there is no table " + tableName(t) + ", please create firstly");
            }
        }
        MyHTable<T> ta = new MyHTable<T>(this, tableName(namespace, t), tds, autoflush);
        return ta;
    }

    /**
     * 从Info元数据表中获取表定义信息
     *
     * @throws IOException
     */
    private void loadTableMeta() throws IOException {
        Connection connection = this.getConnection();
        Table meta = connection.getTable(TableName.valueOf(META_TABLE));

        ResultScanner rscan = meta.getScanner(Bytes.toBytes(META_FAMILY));
        rscan.forEach(m -> {
            Cell cell = m.getColumnLatestCell(Bytes.toBytes(META_FAMILY), Bytes.toBytes(META_COLUMN));
            if (cell != null) {
                tdsmap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        });

        rscan.close();
        meta.close();
        connection.close();
    }

    /**
     * 根据POJO对象创建表
     *
     * @param t
     * @throws IOException
     */
    public void createTable(Class<?> t) throws IOException {
        this.createTable(null, t);
    }

    public void createTable(String namespace, Class<?> t) throws IOException {
        this.createTable(namespace, t, null);
    }

    public void createTable(String namespace, Class<?> t, Compression.Algorithm compress) throws IOException {
        this.createTable(tableName(namespace, t), this.getDefinitionString(t), compress);
    }

    /**
     * 获取POJO对象getXXX方法定义的字段为表的属性字段
     *
     * @param t
     * @return
     */
    private String getDefinitionString(Class<?> t) {
        Method[] methods = t.getDeclaredMethods();
        List<Method> mlist = Arrays.stream(methods)
                .filter(m -> m.getName().startsWith("get") && (!m.getReturnType().equals(void.class)) && m.getParameterCount() == 0)
                .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mlist.size(); i++) {
            if (i != 0)
                sb.append(",");
            String cname = mlist.get(i).getName().substring(3);
            String type = CType.getCType(mlist.get(i).getReturnType()).name();
            sb.append('c').append(i + 1).append(":").append(cname).append(":").append(type);
        }
        return sb.toString();
    }

    /**
     * 通过表名、表定义字符串创建表
     *
     * @param name      表名
     * @param defString 表字段类型定义例如  c1:Name:STRING,c2:Address:STRING,c3:Age:INT  表示c1列，属性名字为name,
     *                  类型是STRING.
     * @throws IOException
     */
    public void createTable(String name, String defString, Compression.Algorithm compress) throws IOException {
        Connection con = this.getConnection();
        Admin admin = con.getAdmin();
        //  check meta table exist, if not exist , create meta table;
        if (!admin.tableExists(TableName.valueOf(META_TABLE))) {
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(META_TABLE));
            htd.addFamily(new HColumnDescriptor(META_FAMILY));
            admin.createTable(htd);
        }
        // check data table exist;
        if (admin.tableExists(TableName.valueOf(name))) {
            throw new IOException("data table " + name + " has already exist");
        }
        // write meta to meta table
        Table meta = con.getTable(TableName.valueOf(META_TABLE));
        Get g1 = new Get(Bytes.toBytes(name));
        Result r1 = meta.get(g1);
        if (r1 != null && !r1.isEmpty()) {
            throw new IOException("the table " + name + " has already exist in meta info table");
        }

        // write table meta data to meta table
        Put p1 = new Put(Bytes.toBytes(name));
        p1.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(META_COLUMN), Bytes.toBytes(defString));
        meta.put(p1);

        // create data table;
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
        if (compress == null)
            htd.addFamily(new HColumnDescriptor(DATA_FAMILY));
        else
            htd.addFamily(new HColumnDescriptor(DATA_FAMILY).setCompressionType(compress));
        admin.createTable(htd);

        // close all;
        meta.close();
        admin.close();
        con.close();
    }

    public void createMetaTable(String namespace, Class<?> t) throws IOException {
        this.createMetaTable(tableName(namespace, t), this.getDefinitionString(t));
    }

    public void createMetaTable(String name, String defString) throws IOException {
        Connection con = this.getConnection();
        Admin admin = con.getAdmin();
        //  check meta table exist, if not exist , create meta table;
        if (!admin.tableExists(TableName.valueOf(META_TABLE))) {
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(META_TABLE));
            htd.addFamily(new HColumnDescriptor(META_FAMILY));
            admin.createTable(htd);
        }
        // check data table exist;
        if (admin.tableExists(TableName.valueOf(name))) {
            logger.info("data table " + name + " has already exist");
        }
        // write meta to meta table
        Table meta = con.getTable(TableName.valueOf(META_TABLE));
        Get g1 = new Get(Bytes.toBytes(name));
        Result r1 = meta.get(g1);
        if (r1 != null && !r1.isEmpty()) {
            logger.info("the table " + name + " has already exist in meta info table");
        } else {
            // write table meta data to meta table
            Put p1 = new Put(Bytes.toBytes(name));
            p1.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(META_COLUMN), Bytes.toBytes(defString));
            meta.put(p1);
        }
        // close all;
        meta.close();
        admin.close();
        con.close();
    }


    /**
     * 删除表
     *
     * @param name 要删除的表的表名
     * @throws IOException
     */
    public void deleteTable(String name) throws IOException {
        Connection con = this.getConnection();
        Admin admin = con.getAdmin();

        // 删除数据表
        if (admin.tableExists(TableName.valueOf(name))) {
            admin.disableTable(TableName.valueOf(name));
            admin.deleteTable(TableName.valueOf(name));
        }
        admin.close();

        // 删除meta表中的数据
        Table meta = con.getTable(TableName.valueOf(META_TABLE));
        Delete del = new Delete(Bytes.toBytes(name));
        del.addFamily(Bytes.toBytes(META_FAMILY));
        meta.delete(del);

        meta.close();
        con.close();
    }

    /**
     * 删除表
     *
     * @param t 通过类型名获取表名
     * @throws IOException
     */
    public void deleteTable(Class<?> t) throws IOException {
        this.deleteTable(tableName(t));
    }

    /**
     * 判断表是否已经存在
     *
     * @param name 表名
     * @return
     * @throws IOException
     */
    boolean tableExist(String name) throws IOException {
        Connection con = null;
        Admin admin = null;
        try {
            con = this.getConnection();
            admin = con.getAdmin();
            if (admin.tableExists(TableName.valueOf(name)))
                return true;
            else return false;
        } finally {
            if (admin != null) admin.close();
            if (con != null) con.close();
        }
    }

    /**
     * 通过类名判断表是否存在
     *
     * @param t 即是表名
     * @return
     * @throws IOException
     */
    boolean tableExist(Class<?> t) throws IOException {
        return this.tableExist(tableName(t));
    }

    public boolean tableExist(String namespace, Class<?> t) throws IOException {
        return tableExist(tableName(namespace, t));
    }

    public void deleteTable(String namespace, Class<?> t) throws IOException {
        this.deleteTable(tableName(namespace, t));
    }

    /**
     * 获取Connection对象
     *
     * @return
     * @throws IOException
     */
    Connection getConnection() throws IOException {
        if (connectionSimplePool.count() == 0) {
            connectionSimplePool.addSource(ConnectionFactory.createConnection(this.configuration, executorService));
        }
        try {
            return connectionSimplePool.getSource();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new IOException("connectionSimplePool.getSource() failed", e);
        }
    }

    String tableName(Class<?> t) {
        return t.getSimpleName().replace('$', '-');
    }

    String tableName(String namespace, Class<?> t) {
        if (namespace == null || namespace.trim().length() == 0) {
            return tableName(t);
        } else {
            return namespace.trim() + ":" + tableName(t);
        }
    }
}