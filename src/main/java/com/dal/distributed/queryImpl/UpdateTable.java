package com.dal.distributed.queryImpl;

import java.util.List;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.constant.RelationalOperators;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.DataUtils;
import com.dal.distributed.utils.DatabaseUtils;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.RemoteVmUtils;

public class UpdateTable {
    private String relationalOp;

    public OperationStatus execute(String query) throws Exception {
        Logger logger = Logger.instance();

        if (Main.databaseName == null || Main.databaseName.isEmpty()) {
            System.out.println("No database selected");
            return null;
        }
        int count = 0;
        OperationStatus operationStatus = null;
        relationalOp = DataUtils.checkRelationalOperator(query);
        String[] sql = query.split("\\s+");
        String tableName = sql[1];
        String updateStatement = query.substring(query.toLowerCase().indexOf("set") + 4);
        String updateColumn = updateStatement.substring(0, updateStatement.indexOf(relationalOp));
        String updateValue = updateStatement.substring(updateStatement.indexOf(relationalOp) + 1,
                updateStatement.indexOf("where") - 1);
        String condition = query.substring(query.toLowerCase().indexOf("where") + 6, query.indexOf(";"));
        String column_name = condition.substring(0, condition.indexOf(relationalOp));
        String value = condition.substring(condition.indexOf(relationalOp) + 1);
        while (updateValue.contains("\'")) {
            updateValue = updateValue.replace("\'", "");
        }
        String databaseName = Main.databaseName;
        int conditionColumnIndex = -1;
        int updateColumnIndex = -1;
        String location = null;
        try {
            location = DatabaseUtils.getTableLocation(databaseName, tableName);
        } catch (IllegalArgumentException ex) {
            logger.error("Database does not exist");
        }
        if (location == null) {
            logger.error("Table does not exist");
            return new OperationStatus(false);
        }
        String filepath = DataConstants.DATABASES_FOLDER_LOCATION + databaseName + "/" + tableName;
        List<List<Object>> data = null;
        if (location.equals("local")) {
            data = new FileOperations().readDataFromPSV(filepath);
        } else if (location.equalsIgnoreCase("remote")) {
            System.out.println("-------------------------Reading-----------------------" + filepath);
            data = RemoteVmUtils.readDataFromPSV(filepath);
        }
        if (data.size() == 1) {
            System.out.println("No data present in the table");
            return new OperationStatus(false);
        }
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
                try {
                    switch (relationalOp) {
                        case RelationalOperators.EQUAL:
                            if (data.get(i).get(conditionColumnIndex).toString().equals(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                            }
                            break;
                        case RelationalOperators.GREATER:
                            if (Integer.parseInt(data.get(i).get(conditionColumnIndex).toString()) > Integer
                                    .parseInt(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                                break;
                            }
                        case RelationalOperators.LESS:
                            if (Integer.parseInt(data.get(i).get(conditionColumnIndex).toString()) < Integer
                                    .parseInt(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                                break;
                            }
                        case RelationalOperators.GREATEREQUAL:
                            if (Integer.parseInt(data.get(i).get(conditionColumnIndex).toString()) >= Integer
                                    .parseInt(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                                break;
                            }
                        case RelationalOperators.LESSEQUAL:
                            if (Integer.parseInt(data.get(i).get(conditionColumnIndex).toString()) <= Integer
                                    .parseInt(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                                break;
                            }
                        case RelationalOperators.NOTEQUAL:
                        case RelationalOperators.NOTEQUAL1:
                        case RelationalOperators.NOTEQUAL2:
                            if (Integer.parseInt(data.get(i).get(conditionColumnIndex).toString()) != Integer
                                    .parseInt(value)) {
                                data.get(i).set(updateColumnIndex, updateValue);
                                count++;
                                break;
                            }
                    }
                    operationStatus = new OperationStatus(true);
                } catch (NumberFormatException e) {
                    operationStatus = new OperationStatus(false);
                }
            }
        }
        if (!Main.isTransaction) {
            if (location.equals("local"))
                new FileOperations().writeDataToPSV(data, filepath);
            else
                new RemoteVmUtils().writeDataToPSV(data, filepath);
            operationStatus = new OperationStatus(true, data, query, filepath, QueryTypes.UPDATE, tableName,
                    Main.databaseName, count);
        } else {
            operationStatus = new OperationStatus(true, data, query, filepath, QueryTypes.UPDATE, tableName,
                    Main.databaseName, count);
        }
        return operationStatus;
    }
}
