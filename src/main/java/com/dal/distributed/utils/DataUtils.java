package com.dal.distributed.utils;

import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.RelationalOperators;

import java.util.regex.Matcher;

public class DataUtils {
    public static String getDataType(String text){
        Matcher matcher = QueryRegex.digitOnlyRegex.matcher(text);
        if(matcher.find())
            return "int";
        else
            return "String";
    }

    public static String checkRelationalOperator(String query)
    {
        String relationalOp;
        if(query.contains(RelationalOperators.GREATER))
        relationalOp=RelationalOperators.GREATER;
        else if(query.contains(RelationalOperators.GREATEREQUAL))
        relationalOp=RelationalOperators.GREATEREQUAL;
        else if(query.contains(RelationalOperators.LESS))
        relationalOp=RelationalOperators.LESS;
        else if(query.contains(RelationalOperators.LESSEQUAL))
        relationalOp=RelationalOperators.LESSEQUAL;
        else if(query.contains(RelationalOperators.NOTEQUAL))
        relationalOp=RelationalOperators.NOTEQUAL;
        else if(query.contains(RelationalOperators.NOTEQUAL1))
        relationalOp=RelationalOperators.NOTEQUAL1;
        else if(query.contains(RelationalOperators.NOTEQUAL2))
        relationalOp=RelationalOperators.NOTEQUAL2;
        else if(query.contains(RelationalOperators.EQUAL))
        relationalOp=RelationalOperators.EQUAL;
        else
        relationalOp=null;
        return relationalOp;
    }
}
