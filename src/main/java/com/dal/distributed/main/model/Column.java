package com.dal.distributed.main.model;

import com.dal.distributed.constant.MiscConstants;

import java.util.Arrays;
import java.util.List;

public class Column {

    private String columnName;

    private String dataType;

    private List<String> constraints;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public List<String> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<String> constraints) {
        this.constraints = constraints;
    }

    public static Column createColumnModel(String columnDef) {
        String [] columnsDefSplit = columnDef.split(MiscConstants.PIPE);
        Column column = new Column();
        column.setColumnName(columnsDefSplit[0]);
        column.setDataType(columnsDefSplit[1]);
        if (columnsDefSplit.length > 2) {
            List<String> columnDefList = Arrays.asList(columnsDefSplit);
            column.setConstraints(columnDefList.subList(2, columnDefList.size()));
        }
        return column;
    }
}
