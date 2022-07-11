package com.spdb.hive.sync.util.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * project：hive-sync
 * package：com.spdb.hive.sync.util
 * author：zhouxh
 * time：2021-04-16 10:33
 * description：
 */
public class DateUtil {

    /**
     * @return 返回yyyy-MM-dd HH:mm:ss格式的时间字符串
     */
    public static String getDate() {
        return getDate(System.currentTimeMillis());
    }

    /**
     * @return 返回yyyy-MM-dd HH:mm:ss格式的时间字符串
     */
    public static String getDate(long date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }

    /**
     * @param startTime 开始时间
     * @param stopTime  结束时间
     * @return 返回时间间隔秒数
     */
    public static long getIntervalSeconds(String startTime, String stopTime) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = sdf.parse(startTime);
            Date stopDate = sdf.parse(stopTime);
            long start = startDate.getTime();
            long stop = stopDate.getTime();
            return (stop - start) / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static String getIntervalFormat(String startTime, String stopTime) {
        long seconds = getIntervalSeconds(startTime, stopTime);
        return getIntervalFormat(seconds);
    }

    public static String getIntervalFormat(long seconds) {
        StringBuilder msg = new StringBuilder("耗时：");
        long hour = seconds / 60 / 60;
        long minute = seconds / 60 % 60;
        long second = seconds % 60;
        if (hour > 0) msg.append(hour).append("小时 ");
        if (minute > 0) msg.append(minute).append("分 ");
        if (second > 0) msg.append(second).append("秒");
        return msg.toString();
    }

    public static void main(String[] args) {
        System.out.println(getIntervalFormat("2021-06-08 07:40:17", "2021-06-08 09:47:27"));
    }

}
