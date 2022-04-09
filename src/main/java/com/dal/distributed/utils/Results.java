package com.dal.distributed.utils;

import java.util.List;

public class Results {

    public static void printResult(List<List<Object>> resultSet)
    {
        for(List<Object> resultVal:resultSet)
        {
            for(Object result:resultVal)
            {
                System.out.print(result.toString()+"  ");
            }
            System.out.println();
        }
    }
    
}
