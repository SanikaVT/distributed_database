package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.model.Pair;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.RemoteVmUtils;

import java.io.File;

public class CreateDatabase {

    Logger logger = Logger.instance();

    public Pair<Boolean, String> execute(String query) throws Exception {
        Pair<Boolean, String> createDbRes = new Pair<>();
        createDbRes.setFirst(false);
        String[] sql = query.split("\\s+");
        if (sql[0].equalsIgnoreCase("create") && sql[1].equalsIgnoreCase("database")) {
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
            FileOperations.writeToExistingFile("tablename|location" + "|", databaseName+".psv", DataConstants.DATABASES_FOLDER_LOCATION);
            FileOperations.writeToExistingFile(databaseName + "|", "databases.psv", DataConstants.DATABASES_FOLDER_LOCATION);

            RemoteVmUtils.createNewFolder(DataConstants.DATABASES_FOLDER_LOCATION, databaseName);
            RemoteVmUtils.writeToExistingFile(databaseName + "|", "databases.psv", DataConstants.DATABASES_FOLDER_LOCATION);

            createDbRes.setFirst(true);
            return createDbRes;
        } else
            return createDbRes;
    }
}