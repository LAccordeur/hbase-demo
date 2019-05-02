package com.rogerguo.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * @Description
 * @Date 2019/4/30 11:09
 * @Created by X1 Carbon
 */
public class DateUtil {

    public static Date parseDateString(String dateString) {
        //DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        //Date date = Date.from(LocalDateTime.parse(dateString, dateFormat).toInstant(ZoneOffset.UTC));
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = simpleDateFormat.parse(dateString);
            return date;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }
}
