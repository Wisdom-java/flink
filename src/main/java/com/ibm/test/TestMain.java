package com.ibm.test;
import com.ibm.utils.DateUtil;
import com.ibm.vo.CountByStripeTotal;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestMain {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long l = System.currentTimeMillis();
        String format = simpleDateFormat.format(l);
        System.out.println(format);
        System.out.println(format.substring(0,17)+"00");
    }
}
