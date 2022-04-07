package com.dal.distributed.queryImpl.model;

public class QueryLog {
    private String flag;
    private String query;
    private String submissionTimestamp;
    private String submittedBy;
    private String tableName;

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getSubmissionTimestamp() {
        return submissionTimestamp;
    }

    public void setSubmissionTimestamp(String submissionTimestamp) {
        this.submissionTimestamp = submissionTimestamp;
    }

    public String getSubmittedBy() {
        return submittedBy;
    }

    public void setSubmittedBy(String submittedBy) {
        this.submittedBy = submittedBy;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return ",\n" +
                "\"QueryLog\": {\n" +
                "\t\"flag\": \"" + flag + "\",\n" +
                "\t\"query\": \"" + query + "\",\n" +
                "\t\"submissionTimestamp\": \"" + submissionTimestamp + "\"\n" +
                "\t\"submittedBy\": \"" + submittedBy + "\"\n" +
                "\t\"tableName\": \"" + tableName + "\"\n" +
                "}";
    }
}
