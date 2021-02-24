package com.umxwe.common.elastic.bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitmapUtil {

    private static final Logger SLOG = LoggerFactory.getLogger(BitmapUtil.class);

    public static RoaringBitmap deserializeBitmap(byte[] arr) {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        try
//                ByteArrayInputStream bai = new ByteArrayInputStream(arr);
//             ObjectInputStream inputStream = new ObjectInputStream(bai)
        {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(baos);
//            oos.write(arr);
//            oos.flush();
//            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
//            roaringBitmap.deserialize(inputStream);
            roaringBitmap.deserialize(ByteBuffer.wrap(arr));

        } catch (IOException e) {
            SLOG.error("deserializeBitmap error", e);
        }
        return roaringBitmap;
    }

    public static byte[] serializeBitmap(RoaringBitmap roaringBitmap) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            boolean runOptimize = roaringBitmap.runOptimize();
            SLOG.debug("run optimize result: [{}]", runOptimize);
            roaringBitmap.serialize(oos);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            SLOG.error("serializeBitmap error", e);
        }
        return null;
    }
}
