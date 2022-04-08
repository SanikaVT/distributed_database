package com.dal.distributed.ERD_Model;
import java.util.*;

public class DataModel
{
    public static ScreenReader reader = new ScreenReader();
    public static FileReadWrite fileReadWrite = new FileReadWrite();
     public static void main(String args[])
     {
         while(true)
         {
             System.out.print("\nData Modelling - Please enter the database name: ");
             String databaseName = reader.readString();
             System.out.println(databaseName);

             if(databaseName.contentEquals("EXIT"))
             {
                 break;
             }
             if(databaseExist(databaseName))
             {
                 createModel(databaseName);
             }
             else
             {
                 System.out.print("\nTry again");;
             }
         }
     }
    public static boolean databaseExist(String databaseName)
    {
       List<String> databaseList = fileReadWrite.getDirectories("databases");
        if (databaseList.contains(databaseName))
        {
            System.out.println("Data Exists");
            return true;
        }
        else
        {
            System.out.println("Database " + databaseName + " does not exist");
            return false;
        }
    }
    public static void createModel(String databaseName)
    {

        Model.extracted(databaseName);
    }

}



