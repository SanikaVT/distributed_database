package com.dal.distributed.queryImpl;

import com.dal.distributed.utils.FileOperations;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class SelectQuery {
    public void execute(String query) throws FileNotFoundException {
        ArrayList fileContent = FileOperations.readPsvFileForQueryOps("./usr/dpg9/databases/dbdbdb/Persons.psv");



    }
}
