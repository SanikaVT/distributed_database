package com.dal.distributed.queryImpl;

import java.io.File;
import java.io.IOException;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class DatabaseCreation {
    public boolean createDatabase(String sql,String filepath) throws IOException{
        String[] query = sql.split("\\s+");
        if(query[0].toLowerCase().equals("create")&& query[1].toLowerCase().equals("database")){
        String databaseName = query[2];
            File[] databases=FileOperations.readFiles(filepath);
            if(databases.length==0)
                return false;
            for(File file:databases){
                if(file.getName().toLowerCase().equals(databaseName.toLowerCase())){
                    System.out.println("Database already exists! Create new one");
                    return false;
                }
            }
            FileOperations.createNewFolder(filepath, databaseName);
            FileOperations.writeToExistingFile(databaseName+"|","databases.psv", AuthConstants.LOGS_FILE_LOCATION);
            return true;
        }
        else
        return false;

}



public String useDatabase(String sql) throws IOException
{
    String[] query = sql.split("\\s+");
    if(query.length==2&&query[0].toLowerCase().equals("use")){
    String dbName=query[1];
    File f=new File(AuthConstants.LOGS_FILE_LOCATION+"databases.psv");
    String dbNames=FileOperations.readFileContent(f);
    if(dbNames.toLowerCase().contains(dbName))
    {
        Main.databaseName=dbName;
        return AuthConstants.SUCCESS;
    }
    else
    return "No database exist";
}
else return "Wrong query";
    
}
}