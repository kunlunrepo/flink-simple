package com.kl.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * description : 时间工具类
 *
 * @author kunlunrepo
 * date :  2024-05-15 09:34
 */
public class TimeUtil {

    /**
     * 格式化
     */
    public static String format(Date time) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        return time.toInstant().atZone(zoneId).toLocalDateTime().format(formatter);
    }

    /**
     * date 转 字符串
     * @param timestamp
     */
    public static String format(long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        String timeStr = formatter.format(new Date(timestamp).toInstant().atZone(zoneId));
        return timeStr;
    }

    /**
     * 字符串 转 date
     * @param time
     */
    public static Date strToDate(String time) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }
}
