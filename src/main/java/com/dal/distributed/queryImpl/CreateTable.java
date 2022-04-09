package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class CreateTable {
    public boolean execute(String query) {
        if (Main.databaseName == null) {
            System.out.println("No database selected");
            return false;
        }
        String[] sql = query.split("\\s+");
        if (sql.length > 3 && sql[0].toLowerCase().equals("create") && sql[1].toLowerCase().equals("table")) {
            String mainStatement = query.substring(query.indexOf(sql[2]));
            String tableName = mainStatement.substring(0, mainStatement.indexOf("("));
            String schema = mainStatement.substring(mainStatement.indexOf("(") + 1, mainStatement.indexOf(";") - 1);
            String columnNames = "";
            String[] columns = schema.split(",");
            int i = 0;
            for (String col : columns) {
                columnNames += col.substring(0, col.indexOf(" "));
                if (i != columns.length - 1)
                    columnNames += "|";
                i++;
            }
            String schemaRow[] = schema.split(",");
            schema = "ColumnName|Datatype|Constraint" + "\n";
            for (int j = 0; j < schemaRow.length; j++) {
                String temp = schemaRow[j];
                temp = temp.replaceFirst(" ", "|").replaceFirst(" ", "|");
                schema += temp;
                if (j != columns.length - 1)
                    schema += "\n";
            }

            FileOperations.writeToExistingFile(columnNames, tableName + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            FileOperations.writeToExistingFile(schema, tableName + "_Schema" + ".psv", DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/");
            //FileOperations.writeToExistingFile(tableName+"|", Main.databaseName+".psv", DataConstants.LOGS_FILE_LOCATION);
            return true;
        } else
            return false;

    }

}

