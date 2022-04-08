package com.dal.distributed.queryImpl.model;

import java.util.List;

public class OperationStatus {
    private boolean status;
    private List<List<Object>> result;
    private String query;
    private String filePath;
    private String queryType;
    private String tableName;
    private boolean isRepeatTable;

    public OperationStatus(boolean status, List<List<Object>> result, String query, String filePath, String queryType, String tableName) {
        this.status = status;
        this.result = result;
        this.query = query;
        this.filePath = filePath;
        this.queryType = queryType;
        this.tableName = tableName;
    }

    public OperationStatus(boolean status) {
        this.status = status;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public List<List<Object>> getResult() {
        return result;
    }

    public void setResult(List<List<Object>> result) {
        this.result = result;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isRepeatTable() {
        return isRepeatTable;
    }

    public void setRepeatTable(boolean isRepeatTable) {
        this.isRepeatTable = isRepeatTable;
    }

}
