import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import com.umxwe.common.utils.TimeUtils;

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
}
