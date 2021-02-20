import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import com.umxwe.common.utils.TimeUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BitmapTest
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/8
 */
public class BitmapTest {



    @Test
    public  void RoaringBitmapIntersectTest() throws Exception {

        RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
        RoaringBitmap rr6 = RoaringBitmap.bitmapOf(3,5,6,9);
        System.out.println("intersection:"+RoaringBitmap.and(rr,rr6));


        TimeUtils.now();
        List<RoaringBitmap> lists=new ArrayList<>();

        RoaringBitmap rr1 = new RoaringBitmap();
        RoaringBitmap rr2 = new RoaringBitmap();
        RoaringBitmap rr3 = new RoaringBitmap();
        RoaringBitmap rr4 = new RoaringBitmap();
        RoaringBitmap rr5 = new RoaringBitmap();

        rr1.add(1L,10000000L);
        rr2.add(9999995L,20000000L);
        rr3.add(9999995L,30000000L);
        rr4.add(9999995L,40000000L);
        rr5.add(9999995L,50000000L);
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
        System.out.println("耗时："+TimeUtils.timeInterval()+" ms");



//
        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap

        rr.or(rr2); //in-place computation
        boolean equals = rror.equals(rr);// true
        if(!equals) throw new RuntimeException("bug");
        // number of values stored?
        long cardinality = rr.getLongCardinality();
        System.out.println(cardinality);
//        for(int i : rr) {
//            System.out.println(i);
//        }
    }

    @Test
    public  void RoaringBitmapTest() throws Exception {
//        RoaringBitmap rr1 = RoaringBitmap.bitmapOf(1,2,3,1000,1,3);
//        RoaringBitmap rr2 = RoaringBitmap.bitmapOf(3,5,6,9,5,6,3,3,1000);
//        System.out.println(rr1 + " "+ rr2);
//        System.out.println("intersection:"+RoaringBitmap.and(rr1,rr2));

        RoaringBitmap roaringBitmap = new RoaringBitmap();

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



        MutableRoaringBitmap rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
        MutableRoaringBitmap rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        // If there were runs of consecutive values, you could
        // call rr1.runOptimize(); or rr2.runOptimize(); to improve compression
        rr1.serialize(dos);
        rr2.serialize(dos);
        dos.close();
        ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
        ImmutableRoaringBitmap rrback1 = new ImmutableRoaringBitmap(bb);
        bb.position(bb.position() + rrback1.serializedSizeInBytes());
        ImmutableRoaringBitmap rrback2 = new ImmutableRoaringBitmap(bb);
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte)((i >> 24) & 0xFF);
        result[1] = (byte)((i >> 16) & 0xFF);
        result[2] = (byte)((i >> 8) & 0xFF);
        result[3] = (byte)(i & 0xFF);
        return result;
    }

}
