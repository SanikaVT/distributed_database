package com.dal.distributed.queryImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class TableCreation {

    FileOperations fileOperations=new FileOperations();
    public String createTable(String sql) throws IOException
    {
        String[] query = sql.split("\\s+");   
        if(query.length>3&& query[0].toLowerCase().equals("create")&&query[1].toLowerCase().equals("table"))
        {
            String mainStatement=sql.substring(sql.indexOf(query[2]));
            String tableName=mainStatement.substring(0,mainStatement.indexOf("("));
            String schema=mainStatement.substring(mainStatement.indexOf("(")+1,mainStatement.indexOf(";")-1);
            String columnNames="";
            String[] columns= schema.split(",");
            for(String col:columns)
            {
                columnNames+=col.substring(0,col.indexOf(" "))+" ";
            }
            schema=schema.replaceAll(",", "|");
            fileOperations.writeToExistingFile(columnNames, tableName+".psv", AuthConstants.DATABASES_FOLDER_LOCATION+Main.databaseName+"/");
            fileOperations.writeToExistingFile(schema, tableName+"_Schema"+".psv", AuthConstants.DATABASES_FOLDER_LOCATION+Main.databaseName+"/");
            fileOperations.writeToExistingFile(tableName+"|", Main.databaseName+".psv", AuthConstants.LOGS_FILE_LOCATION);
            return AuthConstants.SUCCESS;
        }
        else
        return "Wrong query written";

    }
    
}
