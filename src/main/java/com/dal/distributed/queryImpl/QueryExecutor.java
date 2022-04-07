package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.QueryTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class QueryExecutor {
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

    public Map executeQuery(String query){
        // Take care of the logging part

        Map checkQuery = validateQuery(query);
        if(checkQuery.get("isValidate")){
            switch(checkQuery.get("queryType")){
                case QueryTypes.CREATE_DATABASE:
                    // Call
                    break;
                case QueryTypes.USE:
                    // Call
                    break;
                case QueryTypes.CREATE_TABLE:
                    // Call
                    break;
                case QueryTypes.UPDATE:
                    // Call
                    break;
                case QueryTypes.DELETE:
                    // Call
                    break;
                case QueryTypes.INSERT:
                    // Call
                    break;
                case QueryTypes.SELECT:
                    // Call
                    break;
                default:
                    // return inappropriate message
            }
        }

    }
}
