import com.umxwe.common.elastic.bitmap.BitmapUtil;
import com.umxwe.common.utils.TimeUtils;

import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @ClassName BitmapTest
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/8
 */
public class BitmapTest {


    @Test
    public void RoaringBitmapIntersectTest() throws Exception {

        RoaringBitmap rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        RoaringBitmap rr6 = RoaringBitmap.bitmapOf(3, 5, 6, 9);
        System.out.println("intersection:" + RoaringBitmap.and(rr, rr6));
        rr.or(rr6);
        System.out.println("distinct:" + rr);


        TimeUtils.now();
        List<RoaringBitmap> lists = new ArrayList<>();

        RoaringBitmap rr1 = new RoaringBitmap();
        RoaringBitmap rr2 = new RoaringBitmap();
        RoaringBitmap rr3 = new RoaringBitmap();
        RoaringBitmap rr4 = new RoaringBitmap();
        RoaringBitmap rr5 = new RoaringBitmap();

        rr1.add(1L, 10000000L);
        rr2.add(9999995L, 20000000L);
        rr3.add(9999995L, 30000000L);
        rr4.add(9999995L, 40000000L);
        rr5.add(9999995L, 50000000L);
        lists.add(rr1);
        lists.add(rr2);
        lists.add(rr3);
        lists.add(rr4);
        lists.add(rr5);
        for (int i = 1; i < lists.size(); i++) {
            lists.get(0).and(lists.get(i));
        }
        System.out.println(lists.get(0));
        System.out.println(lists.get(0).getLongCardinality());
        System.out.println("耗时：" + TimeUtils.timeInterval() + " ms");


//
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap

        rr.or(rr2); //in-place computation
        boolean equals = rror.equals(rr);// true
        if (!equals) throw new RuntimeException("bug");
        // number of values stored?
        long cardinality = rr.getLongCardinality();
        System.out.println(cardinality);
//        for(int i : rr) {
//            System.out.println(i);
//        }

        long count = 300000;
        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
        while (count >= 0) {
            long num = new Random().nextLong();
            roaring64Bitmap.add(num);
            count--;
        }
        roaring64Bitmap.runOptimize();

        count = 300000;
        Roaring64Bitmap roaring64Bitmap2 = new Roaring64Bitmap();
        while (count >= 0) {
            long num = new Random().nextLong();
            roaring64Bitmap2.add(num);
            count--;
        }
        roaring64Bitmap2.runOptimize();

        long current = System.currentTimeMillis();
        roaring64Bitmap.or(roaring64Bitmap2);

        System.out.println("took:" + (System.currentTimeMillis() - current));
        System.out.println(roaring64Bitmap2);


    }

    @Test
    public void compressBitmapTest() throws IOException {
        long count = 5000000;
        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
        while (count >= 0) {
            long num = new Random().nextLong();
            roaring64Bitmap.add(num);
            count--;
        }
        roaring64Bitmap.runOptimize();
        System.out.println("compress before:" + getNetFileSizeDescription(BitmapUtil.serializeBitmap(roaring64Bitmap).length));

        byte[] compressBitmap = Snappy.compress(BitmapUtil.serializeBitmap(roaring64Bitmap));
        System.out.println("compress after:" + getNetFileSizeDescription(compressBitmap.length));
    }

    @Test
    public void xorandnotTest() {
        RoaringBitmap a = RoaringBitmap.bitmapOf(1, 2, 3, 1000);
        RoaringBitmap b = RoaringBitmap.bitmapOf(3, 5, 6, 9);
        System.out.println("交集intersection:" + RoaringBitmap.and(a, b));
        System.out.println("异或xor:" + RoaringBitmap.xor(a, b));
        System.out.println("不同的andnot:" + RoaringBitmap.andNot(a, b));
    }


    /**
     * 存储单位转换
     *
     * @param size
     * @return
     */
    public static String getNetFileSizeDescription(long size) {
        StringBuffer bytes = new StringBuffer();
        DecimalFormat format = new DecimalFormat("###.0");
        if (size >= 1024 * 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GB");
        } else if (size >= 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB");
        } else if (size >= 1024) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB");
        } else if (size < 1024) {
            if (size <= 0) {
                bytes.append("0B");
            } else {
                bytes.append((int) size).append("B");
            }
        }
        return bytes.toString();
    }

    @Test
    public void RoaringBitmapTest() throws Exception {

        RoaringBitmap roaringBitmap = new RoaringBitmap();
        byte[] temp = "fsfssaf".getBytes();
        byte[] tempnum = intToByteArray(100);
        System.out.println("byte:" + temp);
        System.out.println("byte:" + tempnum);


        ByteArrayInputStream bai = new ByteArrayInputStream(tempnum);
        DataInputStream inputStream = new DataInputStream(bai);
//        ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(ByteBuffer.wrap("澳YUTQUT".getBytes()));
//        DataInputStream dis = new DataInputStream(in);
//        final byte[] convertedBytes = RoaringBitmapUtils.upConvertSerialisedForm(allBytes, offset, length);
//        final ByteArrayInputStream byteIn = new ByteArrayInputStream(convertedBytes);
        int cookie = Integer.reverseBytes(inputStream.readInt());
        System.out.println("cookie:" + cookie);
        roaringBitmap.deserialize(inputStream);
//        roaringBitmap =new ImmutableRoaringBitmap(ByteBuffer.wrap(temp)).toRoaringBitmap();
//        System.out.println(roaringBitmap);

//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.write("1".getBytes());
//        oos.flush();
//        byte[] str = baos.toByteArray();
//        ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
//        ByteArrayInputStream bai = new ByteArrayInputStream(intToByteArray(2));
//             ObjectInputStream inputStream = new ObjectInputStream(bai);


//        roaringBitmap.deserialize(ByteBuffer.wrap("澳YUTQUT".getBytes()));
//        System.out.println("roaringBitmap.deserialize:"+roaringBitmap);


//        MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
//        MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf(2, 3, 1010);
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        DataOutputStream dos = new DataOutputStream(bos);
//        // If there were runs of consecutive values, you could
//        // call rr1.runOptimize(); or rr2.runOptimize(); to improve compression
//        rr1.serialize(dos);
//        rr2.serialize(dos);
//        dos.close();
//        ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
//        ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
//        bb.position(bb.position() + rrback1.serializedSizeInBytes());
//        ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);
    }
//@Test
//    public void SerializeToStringExample(){
//    MutableRoaringBitmap mrb = MutableRoaringBitmap.bitmapOf(1,2,3,1000);
//    System.out.println("starting with  bitmap "+ mrb);
//    ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
//    // If there were runs of consecutive values, you could
//    // call mrb.runOptimize(); to improve compression
//    mrb.serialize(outbb);
//    //
//    outbb.flip();
//    String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
//    ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(serializedstring));
//    ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
//    System.out.println("read bitmap "+ irb);
//}

    @Test
    public void encodedecodetest() throws IOException {
        int num = 1870095726;
        String str = "陕JOORRU";

        System.out.println(intToByteArray(num));
        System.out.println(str.getBytes());
        System.out.println(byteArrayToInt(str.getBytes()));


        long encode = GeoTileUtils.longEncode(179.999, 89.999, 29);
        System.out.println(encode);
        String decode = GeoTileUtils.stringEncode(encode);
        System.out.println(decode);

        System.out.println(GeoTileUtils.longEncode(decode));

        System.out.println(GeoTileUtils.stringEncode(encode)
        );

        String fromJedi = "陕JOORRU";
        byte[] fromRawData = fromJedi.getBytes();

        BigInteger number = new BigInteger(fromRawData);

        System.out.println(number);

        byte[] toRawData = number.toByteArray();
        String toJedi = new String(toRawData);

        System.out.println("The new String is: " + toJedi);


        String code = "AQcAAAAAAAAAAAIAAQAAAIBkAAIAAVwAAAEAAAQAAJeRi4UEAAAAAGRcAIVlAAAAAAAAAAAAAgAAAABbWgQAAAAAZFwAi1oBAAAAAAAAAAQAAAAAZFwAi1sCAAAAAAAAAAQAAAAAZFwAkVEDAAAAAAAAAAQAAAAAZFwAl0cEAAAAAAAAAAQAAAAAZFwBVgMFAAAAAAAAAAQAAAAAgJf6V7AGAAAAAAAAAAEAAAD+CQAAAAECAQAAAGhiAQIBAAAAfIABAgEAAAB4UQECAQAAALAxAQICAAAAlAbABwECAQAAAPgmAQIBAAAARg8AAAcAAAAAAAAAAAAAAAYAAAA=";


        Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();

        byte[] bytes = code.getBytes();
        System.out.println(bytes);
        roaring64Bitmap.deserialize(ByteBuffer.wrap(bytes));

        System.out.println(roaring64Bitmap);

    }


    public long encodeStringToLong(String str) {

        Map<String, Integer> provice = new HashMap<>();


        return 0;

    }

    public String decodeLongToString(Long number) {


        return "";

    }

    /**
     * byte[]转int
     *
     * @param bytes 需要转换成int的数组
     * @return int值
     */
    public int byteArrayToInt(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    class ByteBufferBackedInputStream extends InputStream {

        ByteBuffer buf;

        ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int available() throws IOException {
            return buf.remaining();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return 0xFF & buf.get();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            int len = Math.min(bytes.length, buf.remaining());
            buf.get(bytes, 0, len);
            return len;
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }

        @Override
        public long skip(long n) {
            int len = Math.min((int) n, buf.remaining());
            buf.position(buf.position() + (int) n);
            return len;
        }
    }

    @Test
    public void hash() {
//        Cryptography crypto = new CriyptographyImpl();
//        String value = "";
//        String encryptedValue = new String();
//        String decryptedValue = new String();
//        encryptedValue = crypto.encrypt(value);
//        System.out.println("\n"+value+" encrypted to "+encryptedValue);
//        decryptedValue = crypto.decrypt(encryptedValue);
//        System.out.println("\n"+encryptedValue+" decrypted to "+decryptedValue);

//        int hash = 0;
//        int offset = 'a' - 1;
//        for(string::const_iterator it=s.begin(); it!=s.end(); ++it) {
//            hash = hash << 1 | (*it - offset);
//        }
        //湘a126544
        //149812122323
    }


}
