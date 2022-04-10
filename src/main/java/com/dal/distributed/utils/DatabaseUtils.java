package com.dal.distributed.utils;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.MiscConstants;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DatabaseUtils {

    private static final String  SCHEMA_FILE_SUFFIX = "_Schema.psv";

    private static final String DATA_FILE_SUFFIX = ".psv";

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
}
