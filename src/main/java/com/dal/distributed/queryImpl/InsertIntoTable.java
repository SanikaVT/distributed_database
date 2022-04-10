package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.FileOperations;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class InsertIntoTable {

    Logger logger = Logger.instance();
    FileOperations fileOperations = new FileOperations();

    public OperationStatus execute(String sql) {
        OperationStatus operationStatus = null;
        boolean isTableExist = false;
        String[] query = sql.split("\\s+");

        // If user enters schema.tableName
        String[] table = query[2].split("\\.");
        String tableName = null;
        String databaseName = null;

        // If user has executed "USE databaseName"
        if (table.length == 2) {
            databaseName = table[0];
            tableName = table[1];
        } else if (!Main.databaseName.isEmpty()) {
            databaseName = Main.databaseName;
            tableName = table[0];
        } else {
            logger.error("No database selected.");
            return new OperationStatus(false);
        }

        File file[] = fileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION + databaseName);
        if (null == file) {
            logger.error("Unknown database " + databaseName);
            return new OperationStatus(false, databaseName);
        }
        String finalValue = null;
        for (File f : file) {
            if (f.getName().toLowerCase().contains(tableName.toLowerCase())) {
                isTableExist = true;

                String[] values = extractValuesFromQuery(sql);

                List<List<Object>> schema = fileOperations.readDataFromPSV(
                        DataConstants.DATABASES_FOLDER_LOCATION + databaseName + "/" + tableName + "_Schema.psv");

                if (values.length != schema.size() - 1) {
                    logger.error("Fields count mismatch: Expected " + (schema.size() - 1) + " fields but received "
                            + values.length);
                    return new OperationStatus(false, databaseName);
                }

                // Primary Key already exists in the database
                if (checkForPrimaryKeyConstraint(f.getPath(), schema, values)) {
                    return new OperationStatus(false, databaseName);
                }

                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                    if (schema.get(i + 1).get(1).equals("int")) {
                        String value;
                        // store value = 5 instead of "5"
                        if (values[i].contains("'")) {
                            Matcher matcher = QueryRegex.valueBetweenQuotes.matcher(values[i]);
                            value = matcher.group();
                        } else {
                            value = values[i];
                        }
                        try {
                            Integer.parseInt(value);
                        } catch (NumberFormatException ex) {
                            logger.error("Incorrect integer value: '" + values[i] +
                                    "' for column '" + schema.get(i + 1).get(0) + "'");
                            return new OperationStatus(false, databaseName);
                        }
                    }
                }
                finalValue = Arrays.stream(values).collect(Collectors.joining("|"));
            }
            if (!Main.isTransaction) {
                fileOperations.writeStringToPSV(finalValue, f.getPath());
                operationStatus = new OperationStatus(true, null, sql, f.getPath(), QueryTypes.INSERT, tableName,
                        databaseName, 1);
            } else {
                List<List<Object>> result = new ArrayList<>();
                List<Object> resultVal = new ArrayList();
                resultVal.addAll(Arrays.asList(finalValue.split("|")));
                result.add(resultVal);
                operationStatus = new OperationStatus(true, result, sql, f.getPath(), QueryTypes.INSERT, tableName,
                        databaseName, 1);
            }
            break;
        }
        if (!isTableExist) {
            logger.error("Table '" + databaseName + "." + query[2] + "' doesn't exist");
        }
        return operationStatus;
    }

    private Boolean checkForPrimaryKeyConstraint(String path, List<List<Object>> schema, String[] value) {
        String primaryKey = getPrimaryKeyColumnName(schema);
        List<List<Object>> existingFile = fileOperations.readDataFromPSV(path);

        // Check for primary key location in the file
        int primaryKeyIndex = 0;
        for (int i = 0; i < existingFile.get(0).size(); i++) {
            if (primaryKey.equalsIgnoreCase(existingFile.get(0).get(i).toString())) {
                primaryKeyIndex = i;
            }
        }

        // Check for primary key constraint
        for (int i = 1; i < existingFile.size(); i++) {
            if (existingFile.get(i).get(primaryKeyIndex).toString().equalsIgnoreCase(value[primaryKeyIndex])) {
                logger.error("Duplicate entry '" + value[primaryKeyIndex] + "' for key '" + primaryKey + "'");
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    private String getPrimaryKeyColumnName(List<List<Object>> schema) {
        String primaryKey = null;

        String constraint;
        for (int i = 1; i < schema.size(); i++) {
            if (schema.get(i).size() < 3)
                continue;
            constraint = (String) schema.get(i).get(2);
            if (constraint.equalsIgnoreCase("PRIMARY KEY")) {
                primaryKey = schema.get(i).get(0).toString();
            }
        }
        return primaryKey;
    }

    private String[] extractValuesFromQuery(String query) {
        String[] values = new String[0];
        Matcher matcher = QueryRegex.insertDataIntoTable.matcher(query);
        if (matcher.find()) {
            values = matcher.group(4).replaceAll("\"", "").split(",");
        }
        return values;
    }
}
