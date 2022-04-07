package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class InsertIntoTable {

    Logger logger = Logger.instance();

    public String execute(String sql) {
        boolean isTableExist = false;
        String[] query = sql.split("\\s+");

        //If user enters schema.tableName
        String[] table = query[2].split("\\.");
        String tableName = null;
        String databaseName = null;

        //If user has executed "USE databaseName"
        if (table.length == 2) {
            databaseName = table[0];
            tableName = table[1];
        } else if (!Main.databaseName.isEmpty()) {
            databaseName = Main.databaseName;
        } else {
            logger.error("No database selected.");
            return AuthConstants.FAILURE;
        }

        FileOperations fileOperations = new FileOperations();
        File file[] = fileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION + databaseName);
        if (null == file) {
            logger.error("Unknown database " + databaseName);
            return AuthConstants.FAILURE;
        }
        String finalValue = null;
        for (File f : file) {
            if (f.getName().toLowerCase().contains(tableName.toLowerCase())) {
                isTableExist = true;

                String[] values = extractValuesFromQuery(sql);

                //Read from schema file
                List<List<Object>> schema =
                        fileOperations.readDataFromPSV(
                                DataConstants.DATABASES_FOLDER_LOCATION + databaseName + "/" + tableName + "_Schema.psv");

                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                    if (schema.get(i + 1).get(1).equals("int")) {
                        String value;
                        if (values[i].contains("\"")) {
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
                            return AuthConstants.FAILURE;
                        }
                    }
                }
                finalValue = Arrays.stream(values).collect(Collectors.joining("|"));
            }
            fileOperations.writeStringToPSV(finalValue, f.getPath());
            break;
        }
        if (!isTableExist) {
            logger.error("Table '" + query[2] + "' doesn't exist");
        }
        return AuthConstants.SUCCESS;
    }

    private String[] extractValuesFromQuery(String query) {
        String[] values = new String[0];
        Matcher matcher = QueryRegex.extractValuesFromInsert.matcher(query);
        if (matcher.find()) {
            values = matcher.group().replaceAll("\"", "").split(",");
        }
        return values;
    }
}
