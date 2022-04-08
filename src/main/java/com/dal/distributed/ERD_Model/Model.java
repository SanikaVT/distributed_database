package com.dal.distributed.ERD_Model;

import java.util.HashMap;
import java.util.List;

public class Model {
    static void extracted(String databaseName) {
        String path = "databases/" + databaseName;
        String dataModelFile = "ERD/" + databaseName + "_ERD_MODEL";
        StringBuilder fileName = new StringBuilder();
        fileName.append("#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*Entity Relationship Diagram for database " + databaseName + "#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*" + "\n\n");
        List<String> tableNames = DataModel.fileReadWrite.getDirectories(path);
        fileName.append("Total number of relations/tables in the database " + databaseName + " = " + tableNames.size() + "\n\n"); // have to apply for the size

        HashMap<String, String> tableFirstColumns = new HashMap<String, String>();
        HashMap<String, String> primaryKeys = new HashMap<String, String>();

        for (String tableName : tableNames) {
            int columnNumber = 0;
            String columnName = "";
            String metaData = DataModel.fileReadWrite.readFile(path + "/" + tableName + "/METADATA");
            String metaDataSegements[];
            metaDataSegements = metaData.split("\n");
            for (String segements : metaDataSegements) {
                if (segements.contains("TABLE")) {
                    continue;
                } else if (segements.contains("COLUMN")) {
                    columnName = segements.substring(segements.indexOf("|") + 1).substring(0, segements.substring(segements.indexOf("|") + 1).indexOf("|"));
                    columnNumber++;
                    if (columnNumber == 1) {
                        tableFirstColumns.put(tableName, columnName.toLowerCase());
                    }
                }
            }
        }
        for (String tableName : tableFirstColumns.keySet()) {
            if (!primaryKeys.keySet().contains(tableFirstColumns.get(tableName))) {
                primaryKeys.put(tableFirstColumns.get(tableName), tableName);
            } else {
                primaryKeys.put(tableFirstColumns.get(tableName), primaryKeys.get(tableFirstColumns.get(tableName)) + ", " + tableName);
            }
        }
        for (String tableName : tableNames) {
            int columnNumber = 0;
            String columnName = "";
            String tableMetaData = DataModel.fileReadWrite.readFile(path + "/" + tableName + "/METADATA");
            String tableMetaDataParts[];
            tableMetaDataParts = tableMetaData.split("\n");

            for (String part : tableMetaDataParts) {
                if (part.contains("TABLE")) {
                    continue;
                } else if (part.contains("COLUMN")) {
                    columnName = part.substring(part.indexOf("|") + 1).substring(0, part.substring(part.indexOf("|") + 1).indexOf("|"));
                    columnNumber++;

                    if (columnNumber == 1) {
                        if (primaryKeys.get(columnName.toLowerCase()).contains(",")) {
                            fileName.append("TABLE NAME: " + tableName + " ::" + " COLUMN_NAME_" + columnNumber + " : " + columnName + " (PRIMARY KEY)" + " [Primary/Foreign Key relationship on Column: " + columnName + " among tables: " + primaryKeys.get(columnName.toLowerCase()) + "\n");
                        } else {
                            fileName.append("TABLE NAME: " + tableName + " ::" + " COLUMN_NAME_" + columnNumber + " : " + columnName + " (PRIMARY KEY)" + "\n");
                        }
                    } else {
                        fileName.append("TABLE NAME: " + tableName + " ::" + " COLUMN_NAME_" + columnNumber + " : " + columnName + "\n");
                    }
                } else if (part.contains("RELATION")) {
                    String[] relation_parts = part.substring(part.indexOf("^") + 1).split(" ");
                    String table1 = relation_parts[1];
                    String table2 = relation_parts[4];
                    String cardinality1 = relation_parts[0];
                    String cardinality2 = relation_parts[3];

                    String cardinality = "NOT FOUND";

                    if (cardinality1.equalsIgnoreCase("many") && cardinality2.equalsIgnoreCase("one")) {
                        cardinality = "N:1";
                    } else if (cardinality1.equalsIgnoreCase("many") && cardinality2.equalsIgnoreCase("many")) {
                        cardinality = "N:N";
                    } else if (cardinality1.equalsIgnoreCase("one") && cardinality2.equalsIgnoreCase("many")) {
                        cardinality = "1:N";
                    } else if (cardinality1.equalsIgnoreCase("one") && cardinality2.equalsIgnoreCase("one")) {
                        cardinality = "1:1";
                    }
                    fileName.append("Cardinality = " + cardinality + " from TABLE-" + table1.toUpperCase() + " to TABLE-" + table2.toUpperCase() + "\n");
                }
            }
            fileName.append("\n");
        }
        DataModel.fileReadWrite.overWriteFile(dataModelFile, fileName.toString());
        System.out.println("Done");
    }
}