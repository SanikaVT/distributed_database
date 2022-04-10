package com.dal.distributed.utils;

import java.util.List;

public class Results {

    public static void printResult(List<List<Object>> resultSet) {
        for (List<Object> resultVal : resultSet) {
            for (Object result : resultVal) {
                System.out.format("|%-25s", result.toString());
            }
            System.out.println();
        }
        System.out.println("\n");
    }
}
