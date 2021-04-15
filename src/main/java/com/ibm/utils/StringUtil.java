package com.ibm.utils;

import org.json.JSONObject;

public class StringUtil {
    /**
     * 无引号的字符串转json
     * @param message
     * @return
     */
    public static String stringToJson(String message){
        String[] strs = message.split(",");
        StringBuffer stringBuffer = new StringBuffer("{");
        for (int i = 0; i < strs.length; i++) {
            stringBuffer.append("\""
                    + strs[i].substring(0, strs[i].indexOf(":")) + "\":\""
                    + strs[i].substring(strs[i].indexOf(":") + 1) + "\",");
        }
        stringBuffer.replace(stringBuffer.length() - 1,stringBuffer.length(), "}");
        return stringBuffer.toString();
    }

    /**
     * 返回字段
     * @param jsonObject
     * @param strName
     * @return
     */
    public static String hasString(JSONObject jsonObject,String strName){
        if (jsonObject.has(strName)){
            return jsonObject.getString(strName) + ",";
        }else {
            return "None,";
        }
    }
}
