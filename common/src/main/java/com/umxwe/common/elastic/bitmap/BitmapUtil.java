package com.umxwe.common.elastic.bitmap;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongConsumer;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public class BitmapUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BitmapUtil.class);

    //"湘A1NS20", "湘A2NN30", "湘A2NSV0", "湘A3NST0", "湘A4NS50", "湘ATNS60", "湘A4NS80"

    public static void main(String[] args) {

        System.out.println(stringToAscii("京d2NN30"));
        System.out.println(asciiToString("28248,65,50,78,78,51,48"));
        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
        roaring64Bitmap.add(28248655078838648L);
        roaring64Bitmap.add(38248655078838648L);
        roaring64Bitmap.runOptimize();
        roaring64Bitmap.forEach(new LongConsumer() {
            @Override
            public void accept(long value) {
                System.out.println(value);
            }
        });

        Iterator<Long> iterator=roaring64Bitmap.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }

    }

    public static String stringToAscii(String value) {
        StringBuffer sbu = new StringBuffer();
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            sbu.append((int) chars[i]);
//            if (i != chars.length - 1) {
//                sbu.append((int) chars[i]).append(",");
//            } else {
//                sbu.append((int) chars[i]);
//            }
        }
        return sbu.toString();
    }

    public static String asciiToString(String value) {
        StringBuffer sbu = new StringBuffer();
        String[] chars = value.split(",");
        for (int i = 0; i < chars.length; i++) {
            sbu.append((char) Integer.parseInt(chars[i]));
        }
        return sbu.toString();
    }

    public static Roaring64Bitmap encodeBitmap(String plateno) {
//        Map<String, Integer> map = new HashMap<>();
//        map.put("湘A1NS20", 1);
//        map.put("湘A2NN30", 2);
//        map.put("湘A2NSV0", 3);
//        map.put("湘A3NST0", 4);
//        map.put("湘A4NS50", 5);
//        map.put("湘ATNS60", 6);
//        map.put("湘A4NS80", 7);
//        map.put("湘A4NS91", 8);

        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
        //9223372036854775807
        long i = Long.valueOf(stringToAscii(plateno));
        System.out.println("id:" + i);
        roaring64Bitmap.add(i);
        return roaring64Bitmap;
    }

    public static String decodeBitmap(RoaringBitmap bitmap) {
        return "";
    }


    /**
     * 反序列化
     *
     * @param arr
     * @return
     */
    public static Roaring64Bitmap deserializeBitmap(byte[] arr) {
        Roaring64Bitmap roaringBitmap = new Roaring64Bitmap();
        try
        {
            roaringBitmap.deserialize(ByteBuffer.wrap(arr));
        } catch (IOException e) {
            LOG.error("deserializeBitmap error", e);
        }
        return roaringBitmap;
    }

    /**
     * 序列化
     *
     * @param roaringBitmap
     * @return
     */
    public static byte[] serializeBitmap(Roaring64Bitmap roaringBitmap) {
        try
        {
            long sizeInBytesL = roaringBitmap.serializedSizeInBytes();
            if (sizeInBytesL >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException();
            }
            int sizeInBytesInt = (int) sizeInBytesL;
            ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytesInt).order(ByteOrder.LITTLE_ENDIAN);
            roaringBitmap.serialize(byteBuffer);
            return byteBuffer.array();
        } catch (Exception e) {
            LOG.error("serializeBitmap error", e);
        }
        return null;
    }

}
