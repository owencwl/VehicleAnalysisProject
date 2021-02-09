package com.umxwe.common.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  时间转换类，当前时间转换成yyyy-mm-dd形式
 */
public class TimeUtils {
    private static Logger logger= LoggerFactory.getLogger(TimeUtils.class);

    private static Date nowTime;

    private static long startTime=0;




    /**
     * 返回当前时间戳
     * @return 当前时间戳, 单位 毫秒
     */
    public static long now() {
        startTime=System.currentTimeMillis();
        return startTime;
    }

    /**
     * 返回时间差，一般用来任务执行时间，单位 毫秒
     * 一定先调用now()方法，否则报错处理
     * @return
     */
    public static long timeInterval() throws Exception {
        if(startTime==0){
            throw new IllegalArgumentException ("you must to call TimeUtils.now() function first.");
        }
        return System.currentTimeMillis()-startTime;
    }

    public static String Date2yyyy_MM_dd(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        nowTime = new Date(System.currentTimeMillis());
        String time = dateFormat.format(nowTime);
        return time;
    }


    public static String Long2yyyyMMdd(long timeLong){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        nowTime = new Date(timeLong*1000);
        String time = dateFormat.format(nowTime);
        return time;
    }

    public static String Date2yyyyMMdd(String timeLong){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        nowTime = new Date(Long.valueOf(timeLong)*1000);
        String time = dateFormat.format(nowTime);
        return time;
    }


    public static void main(String[] args) {
        System.out.println(Long2yyyyMMdd(1540884324));
    }
}
