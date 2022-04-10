package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.VMConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.DatabaseUtils;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.RemoteVmUtils;

import java.util.concurrent.ThreadLocalRandom;


import java.io.File;
import java.io.IOException;

public class CreateTable {

    private static Logger logger = Logger.instance();

    public OperationStatus execute(String query) throws Exception {
        if (Main.databaseName == null || Main.databaseName.isEmpty()) {
            System.out.println("No database selected");
            return new OperationStatus(Boolean.FALSE, null);
        }
        int min=0, max=1;
        String location="";
        int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
        if(randomNum==0)
        location=VMConstants.LOCAL;
        else
        location=VMConstants.REMOTE;
        String[] sql = query.split("\\s+");
        if (sql.length > 3 && sql[0].toLowerCase().equals("create") && sql[1].toLowerCase().equals("table")) {
            String mainStatement = query.substring(query.indexOf(sql[2]));
            String tableName = mainStatement.substring(0, mainStatement.indexOf("("));
            if (DatabaseUtils.getTableLocation(Main.databaseName, tableName)!=null) {
                logger.error("Table already exists! choose a different name!");
                return new OperationStatus(Boolean.FALSE, Main.databaseName);
            }
            String schema = mainStatement.substring(mainStatement.indexOf("(") + 1, mainStatement.indexOf(";") - 1);
            String columnNames = "";
            String[] columns = schema.split(",");
            int i = 0;
            for (String col : columns) {
                col = col.trim();
                columnNames += col.substring(0, col.indexOf(" "));
                if (i != columns.length - 1)
                    columnNames += "|";
                i++;
            }
            columnNames += "\n";
            String schemaRow[] = schema.split(",");
            schema = "ColumnName|Datatype|Constraint" + "\n";
            for (int j = 0; j < schemaRow.length; j++) {
                String temp = schemaRow[j].trim();
                temp = temp.replaceFirst(" ", "|").replaceFirst(" ", "|");
                schema += temp;
                if (j != columns.length - 1)
                    schema += "\n";
            }
            if(randomNum==0){
            FileOperations.writeToExistingFile(columnNames, tableName + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            FileOperations.writeToExistingFile(schema, tableName + "_Schema" + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            FileOperations.writeToExistingFile(tableName+"|", Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
            FileOperations.writeToExistingFile("\n"+tableName+"|"+VMConstants.LOCAL , Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
            RemoteVmUtils.writeToExistingFile("\n"+tableName+"|"+VMConstants.REMOTE , Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
        }
        else
        {
            RemoteVmUtils.writeToExistingFile(columnNames, tableName + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            RemoteVmUtils.writeToExistingFile(schema, tableName + "_Schema" + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            RemoteVmUtils.writeToExistingFile(tableName+"|", Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
            FileOperations.writeToExistingFile("\n"+tableName+"|"+VMConstants.REMOTE , Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
            RemoteVmUtils.writeToExistingFile("\n"+tableName+"|"+VMConstants.LOCAL , Main.databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
        }

            return new OperationStatus(Boolean.TRUE, Main.databaseName);
        } else
            return new OperationStatus(Boolean.FALSE, Main.databaseName);
    }

    // private boolean isTableExisted(String databaseName, String tableName) throws Exception {
    //    String fileContent=FileOperations.readFileContent(new File(DataConstants.DATABASES_FOLDER_LOCATION + databaseName));
    //    if(fileContent.contains(tableName))
    //    {
    //         return true;
    //    }
    //    else
    //    {
    //        fileContent=RemoteVmUtils.readFileContent(DataConstants.DATABASES_FOLDER_LOCATION + databaseName);
    //        if(fileContent.contains(tableName))
    //        {
    //            return true;
    //        }
    //    }
    //     return false;
    // }
}

