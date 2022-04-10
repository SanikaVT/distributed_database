package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.FileOperations;

import java.io.File;

public class CreateTable {

    private static Logger logger = Logger.instance();

    public OperationStatus execute(String query) {
        if (Main.databaseName == null || Main.databaseName.isEmpty()) {
            System.out.println("No database selected");
            return new OperationStatus(Boolean.FALSE, null);
        }
        String[] sql = query.split("\\s+");
        if (sql.length > 3 && sql[0].toLowerCase().equals("create") && sql[1].toLowerCase().equals("table")) {
            String mainStatement = query.substring(query.indexOf(sql[2]));
            String tableName = mainStatement.substring(0, mainStatement.indexOf("("));
            if (isTableExisted(Main.databaseName, tableName)) {
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

            FileOperations.writeToExistingFile(columnNames, tableName + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            FileOperations.writeToExistingFile(schema, tableName + "_Schema" + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            //FileOperations.writeToExistingFile(tableName+"|", Main.databaseName+".psv", DataConstants.LOGS_FILE_LOCATION);
            return new OperationStatus(Boolean.TRUE, Main.databaseName);
        } else
            return new OperationStatus(Boolean.FALSE, Main.databaseName);
    }

    private boolean isTableExisted(String databaseName, String tableName) {
        File file[] = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION + databaseName);
        for (File f : file) {
            if (f.getName().toLowerCase().contains(tableName.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}

