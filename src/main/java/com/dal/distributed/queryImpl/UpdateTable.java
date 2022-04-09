package com.dal.distributed.queryImpl;

import java.util.List;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.constant.RelationalOperators;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.DataUtils;
import com.dal.distributed.utils.FileOperations;

public class UpdateTable {
    private String relationalOp;
    public OperationStatus execute(String query) {
        if(Main.databaseName==null){
            System.out.println("No database selected");
            return null;
        }
        OperationStatus operationStatus=null;
        relationalOp=DataUtils.checkRelationalOperator(query);
        String[] sql = query.split("\\s+");
        String tableName = sql[1];
        String updateStatement = query.substring(query.toLowerCase().indexOf("set") + 4);
        String updateColumn = updateStatement.substring(0, updateStatement.indexOf(relationalOp));
        String updateValue = updateStatement.substring(updateStatement.indexOf(relationalOp) + 1, updateStatement.indexOf("where") - 1);
        String condition = query.substring(query.toLowerCase().indexOf("where") + 6,query.indexOf(";"));
        String column_name = condition.substring(0, condition.indexOf(relationalOp));
        String value = condition.substring(condition.indexOf(relationalOp) + 1);
        while(updateValue.contains("\'"))
        {
            updateValue=updateValue.replace("\'", "");
        }
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
                try{
                switch(relationalOp)
                {
                    case RelationalOperators.EQUAL:
                    if (data.get(i).get(conditionColumnIndex).toString().equals(value)) {
                        data.get(i).set(updateColumnIndex, updateValue);
                    }
                    break;
                    case RelationalOperators.GREATER:
                    if(Integer.parseInt(data.get(i).get(conditionColumnIndex).toString())>Integer.parseInt(value)){
                        data.get(i).set(updateColumnIndex, updateValue);
                    break;
                    }
                    case RelationalOperators.LESS:
                    if(Integer.parseInt(data.get(i).get(conditionColumnIndex).toString())<Integer.parseInt(value)){
                        data.get(i).set(updateColumnIndex, updateValue);
                    break;
                    }
                    case RelationalOperators.GREATEREQUAL:
                    if(Integer.parseInt(data.get(i).get(conditionColumnIndex).toString())>=Integer.parseInt(value)){
                        data.get(i).set(updateColumnIndex, updateValue);
                    break;
                    }
                    case RelationalOperators.LESSEQUAL:
                    if(Integer.parseInt(data.get(i).get(conditionColumnIndex).toString())<=Integer.parseInt(value)){
                        data.get(i).set(updateColumnIndex, updateValue);
                    break;
                    }
                    case RelationalOperators.NOTEQUAL:
                    case RelationalOperators.NOTEQUAL1:
                    case RelationalOperators.NOTEQUAL2:
                    if(Integer.parseInt(data.get(i).get(conditionColumnIndex).toString())!=Integer.parseInt(value)){
                        data.get(i).set(updateColumnIndex, updateValue);
                    break;
                    }
                    }
                    operationStatus=new OperationStatus(true);
                }
                catch(NumberFormatException e)
                {
                    operationStatus=new OperationStatus(false);
                }
                }
            }
        if(!Main.isTransaction)
        {
        new FileOperations().writeDataToPSV(data, filepath);
        }
        else
        {
            operationStatus=new OperationStatus(true, data, query, filepath,QueryTypes.UPDATE,tableName);
        }

        return operationStatus;
    }

}
