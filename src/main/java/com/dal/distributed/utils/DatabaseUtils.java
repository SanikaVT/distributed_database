package com.dal.distributed.utils;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.constant.VMConstants;
import com.dal.distributed.main.model.Table;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseUtils {

    private static final String  SCHEMA_FILE_SUFFIX = "_Schema.psv";

    private static final String DATA_FILE_SUFFIX = ".psv";

    private static final String META_DATA_FILE_SUFFIX = ".psv";

    public static List<File> getTableSchemaFiles(String database) {
        String databaseFolder = DataConstants.DATABASES_FOLDER_LOCATION + File.separator + database;
        File [] databaseFiles = FileOperations.readFiles(databaseFolder);
        if (databaseFiles.length == 1)
            return Collections.emptyList();
        List<File> schemaFiles = new ArrayList<>();
        for (File databaseFile: databaseFiles) {
            if (databaseFile.getName().endsWith(SCHEMA_FILE_SUFFIX))
                schemaFiles.add(databaseFile);
        }
        return schemaFiles;
    }

    public static List<String> getColumnDefinitions(String database, File tableSchemaFile) {
        try (FileReader fr = new FileReader(tableSchemaFile);
             BufferedReader br = new BufferedReader(fr)){
            //The buffered reader will now point after header row
            br.readLine();
            List<String> columnDefinitions = new ArrayList<>();
            String colDefinition;
            while ((colDefinition = br.readLine())!=null) {
                columnDefinitions.add(colDefinition);
            }
            return columnDefinitions;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public String getTableNameFromFileName(String tableFileName) {
        return tableFileName.split("_")[0];
    }

    public static File getDataFileForTable(String database, String tableName) {
        String dataFilePath = DataConstants.DATABASES_FOLDER_LOCATION + File.separator + database + File.separator + tableName + DATA_FILE_SUFFIX;
        return new File(dataFilePath);
    }

    public static String getDataFilePathFromTable(String database, String tableName) {
        String dataFilePath = DataConstants.DATABASES_FOLDER_LOCATION + database + File.separator + tableName + DATA_FILE_SUFFIX;
        return dataFilePath;
    }

    public static List<String> getColumnNames(File tableDataFile) {
        try (FileReader fr = new FileReader(tableDataFile);
             BufferedReader br = new BufferedReader(fr);){
             return Arrays.asList(br.readLine().split(MiscConstants.PIPE));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public static String getTableLocation(String databaseName, String tableName) {
        String metaDataFileLocation = DataConstants.DATABASES_FOLDER_LOCATION + databaseName + META_DATA_FILE_SUFFIX;
        File metaDataFile = new File(metaDataFileLocation);
        if (!metaDataFile.exists())
            throw new IllegalArgumentException("Database doesn't exist");
        try (FileReader fr = new FileReader(metaDataFile);
             BufferedReader br = new BufferedReader(fr)){
            //Buffered read will point after header row
            br.readLine();
            String tableInfo;
            while ((tableInfo= br.readLine())!=null) {
                String [] tableInfoArr = tableInfo.split(MiscConstants.PIPE);
                if (tableInfoArr[0].equalsIgnoreCase(tableName))
                    return tableInfoArr[1];
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //throw new IllegalArgumentException("table name doesn't exist in the database");
        return null;
    }

    public static Map<String, String> getTableNames(String databaseName) {
        String metaDataFileLocation = DataConstants.DATABASES_FOLDER_LOCATION + databaseName + META_DATA_FILE_SUFFIX;
        File metaDataFile = new File(metaDataFileLocation);
        if (!metaDataFile.exists())
            throw new IllegalArgumentException("Database doesn't exist");
        Map<String, String> tableNameToLocation = new HashMap<>();
        try (FileReader fr = new FileReader(metaDataFile);
             BufferedReader br = new BufferedReader(fr)){
            //Buffered read will point after header row
            br.readLine();
            String tableInfo;
            while ((tableInfo= br.readLine())!=null) {
                String [] tableInfoArr = tableInfo.split(MiscConstants.PIPE);
                tableNameToLocation.put(tableInfoArr[0], tableInfoArr[1]);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return tableNameToLocation;
    }

    public static List<Table> getRemoteTables(String databaseName) throws Exception {
        Map<String, String> tableNameToLocation = getTableNames(databaseName);
        System.out.println("table names to locations");
        tableNameToLocation.entrySet().stream().forEach(x -> {
            System.out.println(x.getKey() + " " + x.getValue());
        });
        List<String> remoteTableNames = tableNameToLocation.entrySet().stream()
                .filter(x -> VMConstants.REMOTE.equals(x.getValue())).map(x -> x.getKey()).collect(Collectors.toList());
        List<Table> remoteTables = new ArrayList<>();
        for (String tableName: remoteTableNames) {
            String tableSchema = RemoteVmUtils.readFileContent(DataConstants.DATABASES_FOLDER_LOCATION + databaseName + File.separator + tableName + SCHEMA_FILE_SUFFIX);
            List<String> columnStrWithHeaders = Arrays.asList(tableSchema.split("\n"));
            List<String> columnStr = columnStrWithHeaders.subList(1, columnStrWithHeaders.size());
            remoteTables.add(Table.createTableModel(databaseName, tableName, columnStr));
        }
        return remoteTables;
    }
}
