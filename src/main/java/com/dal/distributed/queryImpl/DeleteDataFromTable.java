package com.dal.distributed.queryImpl;

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

import java.util.List;

public class DeleteDataFromTable {
    private String relationalOp;

    public OperationStatus execute(String query) throws Exception {
        Logger logger = Logger.instance();

        if (Main.databaseName == null) {
            System.out.println("No database selected");
            return null;
        }
        int count = 0;
        relationalOp = DataUtils.checkRelationalOperator(query);
        OperationStatus operationStatus = null;
        String[] sql = query.split("\\s+");
        String tablename = sql[2];
        String condition = query.substring(query.toLowerCase().indexOf("where") + 6, query.indexOf(";"));
        String column_name = condition.substring(0, condition.indexOf(relationalOp));
        String value = condition.substring(condition.indexOf(relationalOp) + 1);
        String databaseName = Main.databaseName;
        int columnIndex = -1;
        boolean isRemoved = false;
        String filepath = DataConstants.DATABASES_FOLDER_LOCATION + databaseName + "/" + tablename;
        List<List<Object>> data;
        String location = null;
        try {
            location = DatabaseUtils.getTableLocation(databaseName, tablename);
        } catch (IllegalArgumentException ex) {
            logger.error("Database does not exist");
        }
        if (location == null) {
            logger.error("Table does not exist");
            return new OperationStatus(false);
        }
        if (location.equalsIgnoreCase("local")) {
            data = new FileOperations().readDataFromPSV(filepath);
        } else {
            data = new RemoteVmUtils().readDataFromPSV(filepath);
        }
        if (data.size() == 1) {
            System.out.println("No data present in the table");
            return new OperationStatus(false);
        }
        int rowLength = data.size();
        int columnLength = data.get(0).size();
        for (int i = 0; i < rowLength; i++) {
            isRemoved = false;
            for (int j = 0; j < columnLength; j++) {
                if (isRemoved)
                    break;
                if (i == 0)
                    if (data.get(0).get(j).toString().toLowerCase().equals(column_name.toLowerCase())) {
                        columnIndex = j;
                        break;
                    }
                try {
                    switch (relationalOp) {
                        case RelationalOperators.EQUAL:
                            if (data.get(i).get(columnIndex).toString().equals(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                isRemoved = true;
                            }
                            break;
                        case RelationalOperators.GREATER:
                            if (Integer.parseInt(data.get(i).get(columnIndex).toString()) > Integer.parseInt(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                break;
                            }
                        case RelationalOperators.LESS:
                            if (Integer.parseInt(data.get(i).get(columnIndex).toString()) < Integer.parseInt(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                break;
                            }
                        case RelationalOperators.GREATEREQUAL:
                            if (Integer.parseInt(data.get(i).get(columnIndex).toString()) >= Integer.parseInt(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                break;
                            }
                        case RelationalOperators.LESSEQUAL:
                            if (Integer.parseInt(data.get(i).get(columnIndex).toString()) <= Integer.parseInt(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                break;
                            }
                        case RelationalOperators.NOTEQUAL:
                        case RelationalOperators.NOTEQUAL1:
                        case RelationalOperators.NOTEQUAL2:
                            if (Integer.parseInt(data.get(i).get(columnIndex).toString()) != Integer.parseInt(value)) {
                                data.remove(data.get(i));
                                count++;
                                rowLength -= rowLength;
                                break;
                            }
                            operationStatus = new OperationStatus(true);
                    }
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
            operationStatus = new OperationStatus(true, data, query, filepath, QueryTypes.UPDATE, tablename, Main.databaseName, count);
        } else {
            operationStatus = new OperationStatus(true, data, query, filepath, QueryTypes.UPDATE, tablename, Main.databaseName, count);
        }
        return operationStatus;
    }
}
