package com.dal.distributed.utils;

import com.dal.distributed.constant.QueryRegex;

import java.util.regex.Matcher;

public class DataUtils {
    public static String getDataType(String text){
        Matcher matcher = QueryRegex.digitOnlyRegex.matcher(text);
        if(matcher.find())
            return "int";
        else
            return "String";
    }
}
