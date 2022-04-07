package com.dal.distributed.queryImpl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.main.Main;
import com.dal.distributed.utils.FileOperations;

public class DeleteDataFromTable {
    public boolean execute(String query) throws Exception {
            String[] sql = query.split("\\s+");
            String tablename=sql[2];
            String condition=query.substring(query.toLowerCase().indexOf("where")+6,query.indexOf(";"));
            String column_name=condition.substring(0,condition.indexOf("="));
            String value=condition.substring(condition.indexOf("=")+1);
            String databaseName=Main.databaseName;
            int columnIndex=-1;
            String filepath=DataConstants.DATABASES_FOLDER_LOCATION+databaseName+"/"+tablename;
            List<List<Object>> data=new FileOperations().readDataFromPSV(filepath);
            for(int i=0;i<data.size();i++)
            {
                for(int j=0;j<data.get(0).size();j++)
                {
                    if(i==0)
                    if(data.get(0).get(j).toString().toLowerCase().equals(column_name.toLowerCase()))
                    {
                        columnIndex=j;
                        break;
                    }
                    if(data.get(i).get(columnIndex).toString().equals(value))
                    {
                        data.remove(data.get(i));
                    }

                }
            }
            new FileOperations().writeDataToPSV(data, filepath);

        return true;

    }
}
