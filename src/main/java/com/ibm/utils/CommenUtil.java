package com.ibm.utils;

import org.json.JSONObject;


public class CommenUtil {

    public static Object returnIfExit(JSONObject jsonObject,String keyName){
        Object object = null;
        if (jsonObject.has(keyName)){
            object = jsonObject.get(keyName);
        }
        return object;
    }

}
