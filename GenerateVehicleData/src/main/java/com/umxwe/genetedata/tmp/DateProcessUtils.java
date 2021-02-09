package com.umxwe.genetedata.tmp;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @ClassName DateProcessUtils
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/8
 */
public class DateProcessUtils {
    /**
     * 计算查询时间范围:
     *
     * startStr =20141120
     * endStr=20150108
     * 得到的结果 List<TimeRange> 为
     * 20141120-20141201
     * 20141201-20150101
     * 20150101-20150108
     */
    public static List<TimeRange > getCallLogRanges(String startStr , String endStr){
        try{
            SimpleDateFormat sdfYMD = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat sdfYM = new SimpleDateFormat("yyyyMM");
            DecimalFormat df00 = new DecimalFormat("00");

            //
            List<TimeRange > list = new ArrayList<>();
            //字符串时间
            String startPrefix = startStr.substring(0, 6);

            String endPrefix = endStr.substring(0, 6);
            int endDay = Integer.parseInt(endStr.substring(6, 8));
            //结束点
            String endPoint = endPrefix + df00.format(endDay + 1);

            //日历对象
            Calendar c = Calendar.getInstance();

            //同年月
            if (startPrefix.equals(endPrefix)) {
                TimeRange  range = new TimeRange ();
                range.setStartPoint(startStr);          //设置起始点

                range.setEndPoint(endPoint);            //设置结束点
                list.add(range);
            } else {
                //1.起始月
                TimeRange  range = new TimeRange ();
                range.setStartPoint(startStr);

                //设置日历的时间对象
                c.setTime(sdfYMD.parse(startStr));
                c.add(Calendar.MONTH, 1);
                range.setEndPoint(sdfYM.format(c.getTime()));
                list.add(range);

                //是否是最后一月
                while (true) {
                    //到了结束月份
                    if (endStr.startsWith(sdfYM.format(c.getTime()))) {
                        range = new TimeRange ();
                        range.setStartPoint(sdfYM.format(c.getTime()));
                        range.setEndPoint(endPoint);
                        list.add(range);
                        break;
                    } else {
                        range = new TimeRange ();
                        //起始时间
                        range.setStartPoint(sdfYM.format(c.getTime()));

                        //增加月份
                        c.add(Calendar.MONTH, 1);
                        range.setEndPoint(sdfYM.format(c.getTime()));
                        list.add(range);
                    }
                }
            }
            return list ;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return null ;
    }
    /**
     * 查询给定时间范围内的每一天的日期(yyyy-MM-dd HH:mm:ss")
     *
     * @param startDateStr 开始时间 (yyyy-MM-dd HH:mm:ss")
     * @param endDateStr 结束时间 (yyyy-MM-dd HH:mm:ss")
     * @return List<String>
     */
    public static List<String> handleRangeDate(String startDateStr, String endDateStr) {
        List<String> listDate = new ArrayList<>();
        DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter df2 = DateTimeFormatter.ofPattern("yyyyMMdd");
        try {
            LocalDateTime startDate = LocalDateTime.parse(startDateStr, df1);
            LocalDateTime endDate = LocalDateTime.parse(endDateStr, df1);
            LocalDateTime tempDate = null;
            while (!(LocalDateTime.of(startDate.plusDays(-1).toLocalDate(), LocalTime.MIN)
                    .equals(LocalDateTime.of(endDate.toLocalDate(), LocalTime.MIN)))) {
                tempDate = startDate;
                String format = tempDate.format(df2);
                listDate.add(format);
                startDate = startDate.plusDays(1);
            }
//            System.out.println(listDate.toString());
            return listDate;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
