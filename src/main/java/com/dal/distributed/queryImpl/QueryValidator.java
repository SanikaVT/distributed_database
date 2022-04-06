package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.QueryTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class QueryValidator {
    public Map validateQuery(String query){
        Map result = new HashMap(){{
            put("isValidate", false);
        }};
        Matcher matcher = QueryRegex.createDatabase.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.CREATE_DATABASE);
        }

        matcher = QueryRegex.useDatabase.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.USE);
        }

        matcher = QueryRegex.createTable.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.CREATE_TABLE);
        }

        matcher = QueryRegex.updateTable.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.UPDATE);
        }

        matcher = QueryRegex.deleteDataInTable.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.DELETE);
        }

        matcher = QueryRegex.insertDataIntoTable.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.INSERT);
        }

        matcher = QueryRegex.selectDataFromTable.matcher(query);
        if(matcher.find()){
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.SELECT);
        }
        return result;
    }
}
