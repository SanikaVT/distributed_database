package com.dal.distributed.queryImpl;

import java.util.List;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.FileOperations;

public class UpdateTable {
    public OperationStatus execute(String query) {
        OperationStatus operationStatus=null;
        String[] sql = query.split("\\s+");
        String tableName = sql[1];
        String updateStatement = query.substring(query.toLowerCase().indexOf("set") + 4);
        String updateColumn = updateStatement.substring(0, updateStatement.indexOf("="));
        String updateValue = updateStatement.substring(updateStatement.indexOf("=") + 1, updateStatement.indexOf("where") - 1);
        String condition = query.substring(query.toLowerCase().indexOf("where") + 6,query.indexOf(";"));
        String column_name = condition.substring(0, condition.indexOf("="));
        String value = condition.substring(condition.indexOf("=") + 1);
        String databaseName = Main.databaseName;
        int conditionColumnIndex = -1;
        int updateColumnIndex = -1;
        String filepath = DataConstants.DATABASES_FOLDER_LOCATION + databaseName + "/" + tableName;
        List<List<Object>> data = new FileOperations().readDataFromPSV(filepath);
        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < data.get(0).size(); j++) {
                if (i == 0) {
                    if (data.get(0).get(j).toString().equalsIgnoreCase(column_name.toLowerCase())) {
                        conditionColumnIndex = j;
                    }
                    if (data.get(0).get(j).toString().toLowerCase().trim().equals(updateColumn.trim().toLowerCase())) {
                        updateColumnIndex = j;
                    }
                }
                if (data.get(i).get(conditionColumnIndex).toString().equals(value)) {
                    data.get(i).set(updateColumnIndex, updateValue);
                }

            }
        }
        if(!Main.isTransaction)
        {
        new FileOperations().writeDataToPSV(data, filepath);
        }
        else
        {
            operationStatus=new OperationStatus(true, data, query, filepath,QueryTypes.DELETE,tableName);
        }

        return operationStatus;
    }
}
