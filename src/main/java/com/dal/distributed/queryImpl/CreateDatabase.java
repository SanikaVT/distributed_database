package com.dal.distributed.queryImpl;

import java.io.File;
import java.io.IOException;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.utils.FileOperations;

public class CreateDatabase {

    Logger logger = Logger.instance();

    public boolean execute(String query) throws IOException {
        String[] sql = query.split("\\s+");
        if (sql[0].equalsIgnoreCase("create") && sql[1].equalsIgnoreCase("database")) {
            //Remove the semicolon from database name
            String databaseName = sql[2].substring(0, sql[2].length() - 1).toLowerCase();
            File[] databases = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
            for (File file : databases) {
                if (file.getName().equalsIgnoreCase(databaseName)) {
                    logger.error("Error Code: 1007. Can't create database '" + databaseName + "'; Database exists.");
                    return false;
                }
            }
            FileOperations.createNewFolder(DataConstants.DATABASES_FOLDER_LOCATION, databaseName);
            //FileOperations.writeToExistingFile(databaseName+"|","databases.psv", DataConstants.LOGS_FILE_LOCATION);
            return true;
        } else
            return false;

    }
}