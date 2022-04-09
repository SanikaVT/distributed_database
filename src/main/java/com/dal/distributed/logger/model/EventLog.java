package com.dal.distributed.logger.model;

import com.dal.distributed.constant.DataConstants;
import java.io.*;
import java.util.Arrays;
import java.util.Date;

public class EventLog {

    /**
     * REGISTRATION, LOGIN, LOGOUT, CREATE_DB, CREATE_TABLE
     */
    private String logType;

    private boolean success;

    private Date eventTime;

    private String userId;

    private String databaseName;

    private String tableName;

    public EventLog() {
        this.eventTime = new Date();
    }

    public EventLog(String logType, String userId) {
        this.eventTime = new Date();
        this.setLogType(logType);
        this.setUserId(userId);
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public static void logEvent(EventLog eventLog) {
        String eventLogFilePath = DataConstants.LOGS_FILE_LOCATION + File.separator + DataConstants.EVENT_LOG_FILE_NAME;
        File eventLogFile = new File(eventLogFilePath);
        if (!eventLogFile.exists()) {
            try {
                eventLogFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        boolean emptyFile = true;
        try (FileReader fr = new FileReader(eventLogFile);
             BufferedReader br = new BufferedReader(fr)){
            emptyFile = (br.readLine() == null);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter fw = new FileWriter(eventLogFile, true);
             BufferedWriter bw = new BufferedWriter(fw)){
            if (!emptyFile)
                bw.write(",\n");
            bw.write(eventLog.toString());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void logLogoutEvent(String userId) {
        EventLog logoutEvent = new EventLog();
        logoutEvent.setLogType("LOGOUT");
        logoutEvent.setUserId(userId);
        logoutEvent.setSuccess(true);
        EventLog.logEvent(logoutEvent);
    }

    @Override
    public String toString() {
        StringBuilder eventLogBuilder = new StringBuilder("{");
        eventLogBuilder.append("\n\t");
        eventLogBuilder.append("\"logType\":")
                .append("\"")
                .append(this.getLogType())
                .append("\"");
        eventLogBuilder.append(",\n\t");
        eventLogBuilder.append("\"eventTime\":")
                .append("\"")
                .append(this.getEventTime())
                .append("\"");
        eventLogBuilder.append(",\n\t");
        eventLogBuilder.append("\"success\":").append(this.isSuccess());
        eventLogBuilder.append(",\n\t");
        eventLogBuilder.append("\"userId\":");
        if (this.getUserId() != null)
            eventLogBuilder.append("\"").append(this.getUserId()).append("\"");
        else
            eventLogBuilder.append("null");
        if (Arrays.asList("CREATE_DB", "CREATE_TABLE").contains(this.getLogType())) {
            eventLogBuilder.append(",\n\t");
            eventLogBuilder.append("\"database\":")
                    .append("\"")
                    .append(this.getDatabaseName())
                    .append("\"");
        }
        if (this.getLogType().equals("CREATE_TABLE")) {
            eventLogBuilder.append(",\n\t");
            eventLogBuilder.append("\"table\":")
                    .append("\"")
                    .append(this.getTableName())
                    .append("\"");
        }
        eventLogBuilder.append("\n}");
        return eventLogBuilder.toString();
    }
}
