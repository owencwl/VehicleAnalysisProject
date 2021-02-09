package com.umxwe.common.hbase.utils;

/**
 * @ClassName CType
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public enum CType {
    STRING,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BYTES,
    OBJECT,

    UNKOWN;

    private static Map<String, CType> ssmap;

    static {
        ssmap = new HashMap<String, CType>();
        ssmap.put(STRING.name(), STRING);
        ssmap.put(INT.name(), INT);
        ssmap.put(LONG.name(), LONG);
        ssmap.put(FLOAT.name(), FLOAT);
        ssmap.put(DOUBLE.name(), DOUBLE);
        ssmap.put(BYTES.name(), BYTES);
        ssmap.put(OBJECT.name(), OBJECT);
        ssmap.put(UNKOWN.name(), UNKOWN);
    }

    public static CType getCType(String name) {
        return ssmap.get(name) != null ? ssmap.get(name) : UNKOWN;
    }

    public static Object toObject(CType ctype, byte[] bs) {
        Object arg = null;
        switch (ctype) {
            case STRING:
                arg = Bytes.toString(bs);
                break;
            case INT:
                arg = Bytes.toInt(bs);
                break;
            case LONG:
                arg = Bytes.toLong(bs);
                break;
            case FLOAT:
                arg = Bytes.toFloat(bs);
                break;
            case DOUBLE:
                arg = Bytes.toDouble(bs);
                break;
            case BYTES:
                arg = bs;
                break;
        }
        return arg;
    }

    public static byte[] toBytes(CType ctype, Object val){
        byte[] bs = null;
        switch (ctype) {
            case STRING:
                bs = Bytes.toBytes((String) val);
                break;
            case INT:
                bs = Bytes.toBytes((int) val);
                break;
            case LONG:
                bs = Bytes.toBytes((long) val);
                break;
            case FLOAT:
                bs = Bytes.toBytes((float) val);
                break;
            case DOUBLE:
                bs = Bytes.toBytes((Double) val);
                break;
            case BYTES:
                bs = (byte[]) val;
                break;
            default:
                ;
        }
        return bs;
    }


    public static CType getCType(Class retype) {
        if (retype.equals(String.class))
            return STRING;

        if (retype.equals(int.class) || retype.equals(Integer.class))
            return INT;

        if (retype.equals(long.class) || retype.equals(Long.class))
            return LONG;

        if (retype.equals(float.class) || retype.equals(Float.class))
            return FLOAT;

        if (retype.equals(double.class) || retype.equals(Double.class))
            return DOUBLE;

        if (retype.equals(byte[].class))
            return BYTES;
        return OBJECT;
    }
}