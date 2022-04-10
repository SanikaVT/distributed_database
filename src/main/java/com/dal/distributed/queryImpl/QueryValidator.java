package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.QueryTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class QueryValidator {
    public Map validateQuery(String query) {
        Map result = new HashMap() {
            {
                put("isValidate", false);
            }
        };
        Matcher matcher = QueryRegex.createDatabase.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.CREATE_DATABASE);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.useDatabase.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.USE);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.createTable.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.CREATE_TABLE);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.updateTable.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.UPDATE);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.deleteDataInTable.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.DELETE);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.insertDataIntoTable.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.INSERT);
            result.put("entity", matcher.group(1));
        }

        matcher = QueryRegex.selectDataFromTable.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.SELECT);
            result.put("entity", matcher.group(5));
        }

        matcher = QueryRegex.startTransaction.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.START_TRANSACTION);
            result.put("entity", null);
        }

        matcher = QueryRegex.endTransaction.matcher(query);
        if (matcher.find()) {
            result.replace("isValidate", true);
            result.put("queryType", QueryTypes.END_TRANSACTION);
            result.put("entity", null);
        }
        return result;
    }
}
