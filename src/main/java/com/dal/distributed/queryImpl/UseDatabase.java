package com.dal.distributed.queryImpl;

import java.io.File;
import java.io.IOException;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class UseDatabase {
    public boolean execute(String query) throws IOException {
        String[] sql = query.split("\\s+");
        if (sql.length == 2 && sql[0].equalsIgnoreCase("use")) {
            String dbName = sql[1].substring(0, sql[1].indexOf(";"));
            File[] databases = FileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION);
            boolean isExist=false;
            for (File file : databases) {
                if (file.getName().equalsIgnoreCase(dbName)) {
                   isExist=true;
                }
            }
            if(isExist){
                Main.databaseName=dbName;
            return true;
            }
            // File f = new File(DataConstants.LOGS_FILE_LOCATION + "databases.psv");
            // String dbNames = FileOperations.readFileContent(f);
            // if (dbNames.toLowerCase().contains(dbName)) {
            //     Main.databaseName = dbName;
            //     return true;
            // } 
            else
                return false;
        } else return true;
    }
}
