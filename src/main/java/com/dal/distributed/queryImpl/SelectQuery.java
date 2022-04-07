package com.dal.distributed.queryImpl;

import com.dal.distributed.utils.FileOperations;

import java.util.ArrayList;

public class SelectQuery {
    public void execute(String query) throws Exception {
        ArrayList fileContent = FileOperations.readPsvFileForQueryOps("./usr/dpg9/databases/dbdbdb/Persons.psv");



    }
}
