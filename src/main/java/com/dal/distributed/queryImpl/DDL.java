package com.dal.distributed.queryImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class DDL {
    FileOperations fileOperations=new FileOperations();
    public String insertIntoTable(String sql)
    {
        boolean isTableExist=false;
        String[] query = sql.split("\\s+");
        String tablename=query[2];
        File file[]=fileOperations.readFiles(AuthConstants.DATABASES_FOLDER_LOCATION+Main.databaseName);
        for(File f:file)
        {
            if(f.getName().toLowerCase().contains(tablename))
            {
                isTableExist=true;
            }
        }
        if(!isTableExist)
        return "No table exist";
        else{

            
            return AuthConstants.SUCCESS;

        }
    }

    public List<String> selectQuery()
    {
        List<String> result=new ArrayList<>();
        return result;
    }

    public String updateTable(String sql)
    {
        return AuthConstants.SUCCESS;
    }

    public String deleteRow(String sql)
    {
        return AuthConstants.SUCCESS;
    }
    
}
