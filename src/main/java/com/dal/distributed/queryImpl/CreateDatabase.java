package com.dal.distributed.queryImpl;

import java.io.File;
import java.io.IOException;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.utils.FileOperations;

public class CreateDatabase {
    public boolean execute(String query) throws IOException {
        String[] sql = query.split("\\s+");
        if(sql[0].toLowerCase().equals("create")&& sql[1].toLowerCase().equals("database")){
        String databaseName = sql[2];
            File[] databases=FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
            if(databases.length==0)
                return false;
            for(File file:databases){
                if(file.getName().toLowerCase().equals(databaseName.toLowerCase())){
                    System.out.println("Database already exists! Create new one");
                    return false;
                }
            }
            FileOperations.createNewFolder(DataConstants.DATABASES_FOLDER_LOCATION, databaseName);
            //FileOperations.writeToExistingFile(databaseName+"|","databases.psv", DataConstants.LOGS_FILE_LOCATION);
            return true;
        }
        else
        return false;

    }
}