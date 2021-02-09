package com.umxwe.genetedata.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;

/**
 * @ClassName RandomDataUtil
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */
public class RandomDataUtil {


    /**
     * 随机生成车辆的原始数据
     */
    public static Map<String, Object> randomRecord() {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("rowKey", UUID.randomUUID().toString().replaceAll("-", ""));
        Integer deviceId = new Random().nextInt(2000000);
        result.put("DeviceID", deviceId);
        result.put("plateColorDesc", randomValue("红", "黑 ", "白", "绿", "黄", "灰", "棕"));
        result.put("vehicleClassDesc", randomValue("重型全挂车", "小型车", "微型车", "紧凑车型", "中等车型", "高级车型",
                "三厢车型", "MPV车型", "SUV等车型", "CDV车型"));
        result.put("deviceName", "摄像头设备" + deviceId);
        result.put("PlateNo", deviceId % 100 == 0 ? generateCarID()
                : randomValue("湘A1NS20", "湘A2NN30", "湘A2NSV0", "湘A3NST0", "湘A4NS50", "湘ATNS60", "湘A4NS80"));
        result.put("shotTime", randomDate("2020-01-01", null));
        double lon = Math.random() * Math.PI * 2;
        double lat = Math.acos(Math.random() * 2 - 1);
        result.put("bayonetLongitude", lon);
        result.put("bayonetLatitude", lat);
        result.put("location", lat + "," + lon);
        return result;
    }

    public static String randomValue(String... values) {
        List<String> list = Arrays.asList(values);
        return list.get((int) (Math.random() * list.size()));
    }
    public static String generateCarID() {

        char[] provinceAbbr = { // 省份简称 4+22+5+3
                '京', '津', '沪', '渝', '冀', '豫', '云', '辽', '黑', '湘', '皖', '鲁', '苏', '浙', '赣', '鄂', '甘', '晋',
                '陕', '吉', '闽', '贵', '粤', '青', '川', '琼', '宁', '新', '藏', '桂', '蒙', '港', '澳', '台'};
        String alphas = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"; // 26个字母 + 10个数字

        Random random = new Random(); // 随机数生成器
        String carID = "";
        // 省份+地区代码+· 如 湘A· 这个点其实是个传感器，不过加上美观一些
        carID += provinceAbbr[random.nextInt(34)]; // 注意：分开加，因为加的是2个char
        carID += alphas.charAt(random.nextInt(26));
        // carID += alphas.charAt(random.nextInt(26)) + "·";

        // 5位数字/字母
        for (int i = 0; i < 5; i++) {
            carID += alphas.charAt(random.nextInt(10));
        }
        return carID;
    }
    public static long randomDate(String beginDate, String endDate) {
        Date end = null;
        Date start = null;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            start = format.parse(beginDate);
            if (null == endDate)
                end = new Date();
            else
                end = format.parse(endDate);
            if (start.getTime() >= end.getTime()) {
                return end.getTime();
            }
            long date = random(start.getTime(), end.getTime());
            return date;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Date().getTime();
    }
    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }


    public static String stringToMD5(String plainText) {
        byte[] secretBytes = null;
        try {
            secretBytes = MessageDigest.getInstance("md5").digest(
                    plainText.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("没有这个md5算法！");
        }
        String md5code = new BigInteger(1, secretBytes).toString(16);
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        return md5code;
    }


    /**
     * 生成连续的时间数据，默认从指定日期开始，按秒开始递增，也可以按其他的天，时，分等方式进行递增
     * @param startTime
     * @return
     */
    public static LocalDateTime genContinuousTS(LocalDateTime startTime) {
        if (startTime == null) {
            startTime = LocalDateTime.of(2021, Month.JANUARY, 1, 0, 0, 0);
        }
//        return startTime.plusSeconds(1L);
        return startTime.plusHours(1L);
    }
}
