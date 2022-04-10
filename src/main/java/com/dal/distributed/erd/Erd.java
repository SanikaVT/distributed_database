package com.dal.distributed.erd;

import com.dal.distributed.export.ExportDatabase;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.model.Column;
import com.dal.distributed.main.model.Table;
import com.dal.distributed.utils.DatabaseUtils;
import com.dal.distributed.utils.FileOperations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Erd {

    Logger logger = Logger.instance();

    private static final String DEFAULT_ERD_FILE_LOCATION = "usr/dpg9/output/erd/";

    private static final String DEFAULT_EXPORT_FILE_NAME = "%s_%s.txt";

    public void flow(Scanner sc, String userId) throws Exception {
        logger.info("Enter the database to generate ERD!");
        String dbName = sc.nextLine();
        if (!ExportDatabase.isDatabaseExists(dbName)) {
            return;
        }
        List<File> schemaFiles = DatabaseUtils.getTableSchemaFiles(dbName);
        List<Table> remoteTables = DatabaseUtils.getRemoteTables(dbName);
        List<Table> allTables = new ArrayList<>(remoteTables);
        for (File tableFile: schemaFiles) {
            List<String> columnDefs = DatabaseUtils.getColumnDefinitions(dbName, tableFile);
            Table table = Table.createTableModel(tableFile.getName(), dbName, columnDefs);
            allTables.add(table);
        }
        StringBuilder erdBuilder = new StringBuilder();
        StringBuilder foreignKeyInfo = new StringBuilder();
        for (Table table: allTables) {
            StringBuilder tableInfo = new StringBuilder();
            tableInfo.append(table.getTableName());
            List<Column> columns = table.getColumns();
            tableInfo.append("\n");
            for (Column column: columns) {
                tableInfo.append("\t").append(column.getColumnName())
                        .append("\t").append(column.getDataType());
                if (primaryKeyContraint(column) != -1)
                    tableInfo.append("\t").append("PRIMARY KEY");
                int fkConstraintIndex = foreignKeyConstraint(column);
                if (fkConstraintIndex != -1) {
                    foreignKeyInfo.append(foreignkeyInvolvedTables(table, column, fkConstraintIndex));
                    foreignKeyInfo.append("\n");
                }
                tableInfo.append("\n");
            }
            tableInfo.append("\n");
            erdBuilder.append(tableInfo);
        }
        erdBuilder.append(foreignKeyInfo);
        FileOperations.createNewFolderRecursively(DEFAULT_ERD_FILE_LOCATION);
        String fileName = String.format(DEFAULT_EXPORT_FILE_NAME, new Date().getTime(), dbName);
        File erdFile = new File(DEFAULT_ERD_FILE_LOCATION + File.separator + fileName);
        try (FileWriter fw = new FileWriter(erdFile);
             BufferedWriter bw = new BufferedWriter(fw)){
            bw.write(erdBuilder.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("The erd is generated at: " + DEFAULT_ERD_FILE_LOCATION + fileName);
    }

    private int primaryKeyContraint(Column column) {
        List<String> constraints = column.getConstraints();
        if (constraints == null || constraints.isEmpty())
            return -1;
        for (int i=0; i<constraints.size(); i++) {
            if (constraints.get(i).equalsIgnoreCase("PRIMARY KEY"))
                return i;
        }
        return -1;
    }

    private int foreignKeyConstraint(Column column) {
        List<String> constraints = column.getConstraints();
        if (constraints == null || constraints.isEmpty())
            return -1;
        for (int i=0; i<constraints.size(); i++) {
            if (constraints.get(i).contains("foreign key") || constraints.get(i).contains("FOREIGN KEY"))
                return i;
        }
        return -1;
    }

    private String foreignkeyInvolvedTables(Table originalTable, Column column, int fkConstraintIndex) {
        StringBuilder sb = new StringBuilder(originalTable.getTableName())
                .append("(").append(column.getColumnName()).append(")");
        sb.append("  ").append("------>").append("  ");

        String constraint = column.getConstraints().get(fkConstraintIndex);
        String []constraintArr = constraint.split(" ");
        sb.append(constraintArr[3]);
        return sb.toString();
    }
}
