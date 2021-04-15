package com.ibm.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 输入开始时间和需要的时间间隔，返回开始时间和结束时间，精确到分钟
     * @param startTime 格式如  "2020-08-11 14:10:10"
     * @param timeGap 单位：minute
     */
    public static long[] getTime(String startTime, int timeGap){
        long[] timeArray = new long[2];
        try {
            //时间格式
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //开始的时间
            Date parse = simpleDateFormat.parse(startTime);
            Calendar calendar = Calendar.getInstance();
            calendar.set(Integer.valueOf(startTime.substring(0,4)),
                    Integer.valueOf(startTime.substring(5,7))-1,
                    Integer.valueOf(startTime.substring(8,10)),
                    Integer.valueOf(startTime.substring(11,13)),
                    Integer.valueOf(startTime.substring(14,16)),0);
            long startTimeMillis = calendar.getTimeInMillis();
            calendar.add(Calendar.MINUTE,timeGap); //几分钟之后
            long endTimeMillis = calendar.getTimeInMillis();
            timeArray[0] = startTimeMillis;
            timeArray[1] = endTimeMillis;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timeArray;
    }

    /**
     * 格式化时间 yyyy-MM-dd HH:mm:ss
     * @param timeMilles
     * @return
     */
    public static long[] getTimeArray(long timeMilles){
        long[] timeArray = new long[2];
        timeArray[0] = timeMilles;
        timeArray[1] = timeMilles + 60 * 1000;
        return timeArray;
    }

    /**
     * 使用@timestamp 作为计算时间
     * @param timestamp
     * @return
     */
    public static String dataName(String timestamp) throws ParseException {
        Date timestampData = parseUTCText(timestamp);
        long timestampMill = timestampData.getTime() + 28800000;
        String format = simpleDateFormat.format(timestampMill);
        String secondStr = format.substring(15, 16);
        String flag = "";
        if ("0".equals(secondStr) || "5".equals(secondStr)){
            flag = "_aa";
        }else if ("1".equals(secondStr) || "6".equals(secondStr)){
            flag = "_ab";
        }else if ("2".equals(secondStr) || "7".equals(secondStr)){
            flag = "_ac";
        }else if ("3".equals(secondStr) || "8".equals(secondStr)){
            flag = "_ad";
        }else if ("4".equals(secondStr) || "9".equals(secondStr)){
            flag = "_ae";
        }
        int secondFlag = Integer.valueOf(format.substring(14, 16));
        String startStr = "";
        String endStr = "";
        if (secondFlag >= 0 && secondFlag < 5){
            startStr = "00";
            endStr = "04";
        }else if (secondFlag >= 5 && secondFlag < 10){
            startStr = "05";
            endStr = "09";
        }else if (secondFlag >= 10 && secondFlag < 15){
            startStr = "10";
            endStr = "14";
        }else if (secondFlag >= 15 && secondFlag < 20){
            startStr = "15";
            endStr = "19";
        }else if (secondFlag >= 20 && secondFlag < 25){
            startStr = "20";
            endStr = "24";
        }else if (secondFlag >= 25 && secondFlag < 30){
            startStr = "25";
            endStr = "29";
        }else if (secondFlag >= 30 && secondFlag < 35){
            startStr = "30";
            endStr = "34";
        }else if (secondFlag >= 35 && secondFlag < 40){
            startStr = "35";
            endStr = "39";
        }else if (secondFlag >= 40 && secondFlag < 45){
            startStr = "40";
            endStr = "44";
        }else if (secondFlag >= 45 && secondFlag < 50){
            startStr = "45";
            endStr = "49";
        }else if (secondFlag >= 50 && secondFlag < 55){
            startStr = "50";
            endStr = "54";
        }else if (secondFlag >= 55){
            startStr = "55";
            endStr = "59";
        }



        return format.substring(0,10) + "_" + format.substring(11,13) + ":" + startStr + "_" +
                format.substring(0,10) + "_" + format.substring(11,13) + ":" + endStr + "|" + flag;
    }
    /**
     * 使用timestamp_finish 作为计算时间
     * @param timestamp_finish
     * @return
     */
    public static String dataName(long timestamp_finish){
        String format = simpleDateFormat.format(timestamp_finish);
        int secondFlag = Integer.valueOf(format.substring(14,16));
        String startStr = "";
        String endStr = "";
        if (secondFlag >= 0 && secondFlag < 5){
            startStr = "00";
            endStr = "04";
        }else if (secondFlag >= 5 && secondFlag < 10){
            startStr = "05";
            endStr = "09";
        }else if (secondFlag >= 10 && secondFlag < 15){
            startStr = "10";
            endStr = "14";
        }else if (secondFlag >= 15 && secondFlag < 20){
            startStr = "15";
            endStr = "19";
        }else if (secondFlag >= 20 && secondFlag < 25){
            startStr = "20";
            endStr = "24";
        }else if (secondFlag >= 25 && secondFlag < 30){
            startStr = "25";
            endStr = "29";
        }else if (secondFlag >= 30 && secondFlag < 35){
            startStr = "30";
            endStr = "34";
        }else if (secondFlag >= 35 && secondFlag < 40){
            startStr = "35";
            endStr = "39";
        }else if (secondFlag >= 40 && secondFlag < 45){
            startStr = "40";
            endStr = "44";
        }else if (secondFlag >= 45 && secondFlag < 50){
            startStr = "45";
            endStr = "49";
        }else if (secondFlag >= 50 && secondFlag < 55){
            startStr = "50";
            endStr = "54";
        }else if (secondFlag >= 55){
            startStr = "55";
            endStr = "59";
        }

        return format.substring(0,10) + "_" + format.substring(11,13) + ":" + startStr + "_" +
                format.substring(0,10) + "_" + format.substring(11,13) + ":" + endStr;

    }

    /**
     * UTC时间转data
     * @param text
     * @return
     * @throws ParseException
     */
    public static Date parseUTCText(String text) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (text.indexOf(".") > -1) {
            String prefix = text.substring(0, text.indexOf("."));
            String suffix = text.substring(text.indexOf("."));
            if (suffix.length() >= 5) {
                suffix = suffix.substring(0, 4) + "Z";
            } else {
                int len = 5 - suffix.length();
                String temp = "";
                temp += suffix.substring(0, suffix.length() - 1);
                for (int i = 0; i < len; i++) {
                    temp += "0";
                }
                suffix = temp + "Z";
            }
            text = prefix + suffix;
        } else {
            text = text.substring(0, text.length() - 1) + ".000Z";
        }
        Date date = sdf.parse(text);
        return date;
    }
}
