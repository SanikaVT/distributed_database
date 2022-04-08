package com.dal.distributed.main.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Table implements Comparator<Table> {

    private String tableName;

    private String databaseName;

    private List<Column> columns;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public static Table createTableModel(String tableName, String databaseName, List<String> columnDefs) {
        Table table = new Table();
        table.setDatabaseName(databaseName);
        //to remove .psv with the table name
        table.setTableName(tableName.split("_")[0]);
        List<Column> columns = new ArrayList<>();
        for (String columnDef: columnDefs) {
            Column col = Column.createColumnModel(columnDef);
            columns.add(col);
        }
        table.setColumns(columns);
        return table;
    }

    @Override
    public int compare(Table o1, Table o2) {
        if (o1.getTableName().equals(o2.getTableName()))
            return 0;
        List<Column> o1Columns = o1.getColumns();
        for (Column o1Column : o1Columns) {
            List<String> constraints = o1Column.getConstraints();
            if(constraints == null || constraints.isEmpty())
                continue;
            for (String constraint: constraints) {
                if (constraint.contains(o2.getTableName()))
                    return 1;
            }
        }
        return -1;
    }
}
