package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

import java.io.File;

public class InsertIntoTable {

    FileOperations fileOperations = new FileOperations();

    public String execute(String sql) {
        boolean isTableExist = false;
        String[] query = sql.split("\\s+");
        String tablename = query[2];
        File file[] = fileOperations.readFiles(DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName);
        for (File f : file) {
            if (f.getName().toLowerCase().contains(tablename)) {
                isTableExist = true;
            }
        }
        if (!isTableExist)
            return "No table exist";
        else {
            return AuthConstants.SUCCESS;
        }
    }
}
