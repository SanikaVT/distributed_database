package com.dal.distributed.export;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.constant.VMConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.model.Column;
import com.dal.distributed.main.model.Table;
import com.dal.distributed.utils.DatabaseUtils;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.RemoteVmUtils;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExportDatabase {

    private static Logger logger = Logger.instance();

    private static final String DEFAULT_EXPORT_FILE_LOCATION = "usr/dpg9/output/export/";

    private static final String DEFAULT_EXPORT_FILE_NAME = "%s_%s.sql";

    private static final String INSERT_GENERIC_QUERY_PREFIX = "INSERT INTO %s VALUES ";

    private static final String CREATE_DATABASE_QUERY_PREFIX = "CREATE DATABASE IF NOT EXISTS %s;";

    private static final String USE_DATABASE = "USE %s;";

    private static final String EXPORT_OUTPUT_PROMPT = "Your exported file will be found at: %s";

    public void flow(Scanner sc) throws Exception {
        while (true) {
            logger.info("Please choose any one of the following options:");
            logger.info("\n1. Show Databases list");
            logger.info("2. Export Database");
            logger.info("3. Go back to main menu");
            String option = sc.nextLine();

            switch (option) {
                case "1":
                    displayDatabases();
                    break;
                case "2":
                    logger.info("Please choose a database");
                    String databaseName = sc.nextLine();
                    String fileName = exportStructureAndValue(databaseName);
                    if (fileName != null)
                        logger.info(String.format(EXPORT_OUTPUT_PROMPT, DEFAULT_EXPORT_FILE_LOCATION + fileName));
                    break;
                case "3":
                    break;
                default:
                    logger.error("Please choose a valid input!");
            }

            if("3".equals(option))
                break;
        }
    }

    private void displayDatabases() {
        File[] files = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
        List<String> databaseNames = new ArrayList<>();
        for (File file: files) {
            if(file.isDirectory())
                databaseNames.add(file.getName());
        }
        Collections.sort(databaseNames);
        databaseNames.stream().forEach(logger::info);
    }

    private String exportStructureAndValue(String database) throws Exception {
        if (!isDatabaseExists(database))
            return null;
        List<File> schemaFiles = DatabaseUtils.getTableSchemaFiles(database);
        List<Table> remoteTables = DatabaseUtils.getRemoteTables(database);
        if ((schemaFiles == null || schemaFiles.isEmpty()) && (remoteTables == null || remoteTables.isEmpty())) {
            logger.info("Selected database is empty! Please choose another one to export");
            return null;
        }
        List<Table> tables = new ArrayList<>(remoteTables);
        for (File tableFile: schemaFiles) {
            List<String> columnDefs = DatabaseUtils.getColumnDefinitions(database, tableFile);
            Table table = Table.createTableModel(tableFile.getName(), database, columnDefs);
            tables.add(table);
        }

        //sort tables based on the foreign keys
        Collections.sort(tables, (o1, o2) -> {
            if (o1.getTableName().equals(o2.getTableName()))
                return 0;
            List<Column> o1Columns = o1.getColumns();
            for (Column o1Column : o1Columns) {
                List<String> constraints = o1Column.getConstraints();
                if (constraints == null || constraints.isEmpty())
                    continue;
                for (String constraint: constraints) {
                    if (constraint.contains(o2.getTableName()))
                        return 1;
                }
            }
            return -1;
        });
        return exportDataToSqlFile(database, tables);
    }

    public static boolean isDatabaseExists(String databaseName) {
        File[] databases = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
        boolean isExist=false;
        for (File file : databases) {
            if (file.getName().equalsIgnoreCase(databaseName)) {
                isExist=true;
            }
        }
        if(!isExist){
            logger.error("Error Code: 1007. Can't export database '" + databaseName + "'; Database doesn't exists.");
            return false;
        }
        return true;
    }

    private String exportDataToSqlFile(String database, List<Table> tables) throws Exception {
        FileOperations.createNewFolderRecursively(DEFAULT_EXPORT_FILE_LOCATION);
        String fileName = String.format(DEFAULT_EXPORT_FILE_NAME, new Date().getTime(), database);
        File exportSqlFile = new File(DEFAULT_EXPORT_FILE_LOCATION + File.separator + fileName);
        try ( FileWriter fw = new FileWriter(exportSqlFile);
              BufferedWriter bw = new BufferedWriter(fw);){
            bw.write(String.format(CREATE_DATABASE_QUERY_PREFIX, database));
            bw.write("\n");
            bw.write(String.format(USE_DATABASE, database));
            bw.write("\n");
            for (Table table: tables) {
                String tableLocation = DatabaseUtils.getTableLocation(database, table.getTableName());
                System.out.println(table.getTableName() + " is in: " + tableLocation);
                boolean isLocal = !VMConstants.REMOTE.equals(tableLocation);
                System.out.println(table.getTableName() + " is in local: " + isLocal);
                String tableQueries = generateCreateTableAlongWithData(database, table, isLocal);
                bw.write(tableQueries);
                bw.write("\n");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return fileName;
    }


    private String generateCreateTableAlongWithData(String database, Table table, boolean isLocal) throws Exception {
        String createTableQuery = generateCreateTable(database, table);
        String insertQueries = isLocal? generateInsertData(database, table): generateInsertDataForRemoteTable(database, table);
        StringBuilder createAndInsertQueriesTable = new StringBuilder(createTableQuery);
        createAndInsertQueriesTable.append("\n");
        createAndInsertQueriesTable.append(insertQueries);
        return createAndInsertQueriesTable.toString();
    }

    private String generateInsertDataForRemoteTable(String database, Table table) throws Exception {
        String dataFilePath = DatabaseUtils.getDataFilePathFromTable(database, table.getTableName());
        String data = RemoteVmUtils.readFileContent(VMConstants.projectPath + dataFilePath);
        List<String> rowsWithHeader = Arrays.asList(data.split("\n"));
        List<String> columnNames = table.getColumns().stream()
                .map(Column::getColumnName).collect(Collectors.toList());
        Map<String, Column> columnNameToColumn = table.getColumns().stream()
                .collect(Collectors.toMap(Column::getColumnName, Function.identity()));
        if (rowsWithHeader.size() == 1)
            return "\n";
        String genericInsertQueryPrefix = String.format(INSERT_GENERIC_QUERY_PREFIX, table.getTableName());
        StringBuilder insertQueryBuilder = new StringBuilder(genericInsertQueryPrefix);
        for (int i=1; i<rowsWithHeader.size(); i++) {
            insertQueryBuilder.append(generateRowValuesForInsert(rowsWithHeader.get(i).split(MiscConstants.PIPE), columnNames, columnNameToColumn));
        }
        insertQueryBuilder.deleteCharAt(insertQueryBuilder.length()-1);
        insertQueryBuilder.append(";");
        return insertQueryBuilder.toString();
    }

    private String generateCreateTable(String database, Table table) {
        // CREATE DATABASE IF NOT EXISTS <db_name>;
        StringBuilder createTableQueryBuilder = new StringBuilder();
        createTableQueryBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s", database, table.getTableName()));
        createTableQueryBuilder.append("(");
        createTableQueryBuilder.append("\n");
        createTableQueryBuilder.append(generateColumnDefinitionQuery(table));
        createTableQueryBuilder.append("\n");
        createTableQueryBuilder.append(");");
        return createTableQueryBuilder.toString();
    }

    private String generateColumnDefinitionQuery(Table table) {
        StringBuilder columnDefinitions = new StringBuilder();
        for (Column column: table.getColumns()) {
            columnDefinitions.append("\t");
            columnDefinitions.append(column.getColumnName());
            columnDefinitions.append(" ");
            columnDefinitions.append(column.getDataType());
            columnDefinitions.append(" ");
            boolean fkConstraint = false;
            if (column.getConstraints() != null && !column.getConstraints().isEmpty()) {
                String constraints = column.getConstraints()
                                    .stream()
                                    .filter(constraint -> !constraint.contains("FOREIGN KEY"))
                                    .collect(Collectors.joining(" "));
                fkConstraint = column.getConstraints()
                                        .stream()
                                        .anyMatch(constraint -> constraint.contains("FOREIGN KEY"));
                columnDefinitions.append(constraints);
            }
            columnDefinitions.append(",\n");
            if (fkConstraint) {
                String fkConstraintStr = column.getConstraints().stream().filter(constraint -> constraint.contains("FOREIGN KEY")).findAny().get();
                columnDefinitions.append("\t");
                columnDefinitions.append(getForeignKeyConstraint(fkConstraintStr, column.getColumnName()));
                columnDefinitions.append(",\n");
            }
        }
        columnDefinitions.deleteCharAt(columnDefinitions.length()-1);
        columnDefinitions.deleteCharAt(columnDefinitions.length()-1);
        return columnDefinitions.toString();
    }

    private String getForeignKeyConstraint(String fkConstraint, String columnName) {
        String [] fkSplit = fkConstraint.split(" ");
        StringBuilder fkConstraintCode = new StringBuilder();
        for (int i=0; i<fkSplit.length; i++) {
            if (i == 2) {
                fkConstraintCode.append("(").append(columnName).append(")");
                fkConstraintCode.append(" ");
            }
            fkConstraintCode.append(fkSplit[i]);
            fkConstraintCode.append(" ");
        }
        fkConstraintCode.deleteCharAt(fkConstraintCode.length()-1);
        return fkConstraintCode.toString();
    }

    private String generateInsertData(String database, Table table) {
        File dataFile = DatabaseUtils.getDataFileForTable(database, table.getTableName());
        List<String> columnNames = DatabaseUtils.getColumnNames(dataFile);
        Map<String, Column> columnNameToColumn = table.getColumns().stream()
                .collect(Collectors.toMap(Column::getColumnName, Function.identity()));
        String genericInsertQueryPrefix = String.format(INSERT_GENERIC_QUERY_PREFIX, table.getTableName());
        boolean isDataPresent = false;
        try (FileReader fr = new FileReader(dataFile);
             BufferedReader br = new BufferedReader(fr);) {
            // Buffered reader will now point after the header row
            br.readLine();
            String line;
            // This builds the entire insert query
            StringBuilder dataQueryBuilder = new StringBuilder(genericInsertQueryPrefix);
            while ((line = br.readLine()) != null) {
                isDataPresent = true;
                String [] rowData = line.split(MiscConstants.PIPE);
                //This builds only comma separated list for a single row.
                StringBuilder rowDataBuilder = new StringBuilder("(");
                for (int i=0; i<columnNames.size(); i++) {
                    String columnName = columnNames.get(i);
                    String data = rowData[i];
                    String dataType = columnNameToColumn.get(columnName).getDataType();
                    if (dataType.contains("int")) {
                        rowDataBuilder.append(data);
                    }
                    else {
                        rowDataBuilder.append("'");
                        rowDataBuilder.append(data);
                        rowDataBuilder.append("'");
                    }
                    rowDataBuilder.append(",");
                }
                rowDataBuilder.deleteCharAt(rowDataBuilder.length()-1);
                rowDataBuilder.append("),");
                dataQueryBuilder.append(rowDataBuilder);
            }
            if (!isDataPresent)
                return "";
            dataQueryBuilder.deleteCharAt(dataQueryBuilder.length()-1);
            dataQueryBuilder.append(";");
            return dataQueryBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    private String generateRowValuesForInsert(String [] rowData, List<String> columnNames, Map<String, Column> columnNameToColumn) {
        StringBuilder rowDataBuilder = new StringBuilder("(");
        for (int i=0; i<columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String data = rowData[i];
            String dataType = columnNameToColumn.get(columnName).getDataType();
            if (dataType.contains("int")) {
                rowDataBuilder.append(data);
            }
            else {
                rowDataBuilder.append("'");
                rowDataBuilder.append(data);
                rowDataBuilder.append("'");
            }
            rowDataBuilder.append(",");
        }
        rowDataBuilder.deleteCharAt(rowDataBuilder.length()-1);
        rowDataBuilder.append("),");
        return rowDataBuilder.toString();
    }

}
