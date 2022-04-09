package com.dal.distributed.queryImpl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.model.Pair;
import com.dal.distributed.utils.FileOperations;

public class CreateDatabase {

    Logger logger = Logger.instance();

    public Pair<Boolean, String> execute(String query) throws IOException {
        Pair<Boolean, String> createDbRes = new Pair<>();
        createDbRes.setFirst(false);
        String[] sql = query.split("\\s+");
        if (sql[0].equalsIgnoreCase("create") && sql[1].equalsIgnoreCase("database")) {
            //Remove the semicolon from database name
            String databaseName = sql[2].substring(0, sql[2].length() - 1).toLowerCase();
            createDbRes.setSecond(databaseName);
            File[] databases = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
            for (File file : databases) {
                if (file.getName().equalsIgnoreCase(databaseName)) {
                    logger.error("Error Code: 1007. Can't create database '" + databaseName + "'; Database exists.");
                    return createDbRes;
                }
            }
            FileOperations.createNewFolder(DataConstants.DATABASES_FOLDER_LOCATION, databaseName);
            FileOperations.writeToExistingFile(databaseName+"|","databases.psv", DataConstants.LOGS_FILE_LOCATION);
            createDbRes.setFirst(true);
            return createDbRes;
        } else
            return createDbRes;

    }
}