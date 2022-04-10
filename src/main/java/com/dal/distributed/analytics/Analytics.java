package com.dal.distributed.analytics;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.queryImpl.model.QueryLog;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.RemoteVmUtils;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;

public class Analytics {
    Logger logger = Logger.instance();

    public void analyze(Scanner scanner) throws Exception {
        while (true) {
            logger.info("\nPlease choose any one of the following options:");
            logger.info("1. Analytics");
            logger.info("2. Exit");
            String input = scanner.nextLine();

            switch (input) {
                case "1":
                    logger.info("Please input a query for the selected option: ");
                    String query = scanner.nextLine();
                    analyzeQueries(query);
                    break;
                case "2":
                    break;
                default:
                    logger.error("Please choose a valid input!");
            }
            if ("2".equals(input))
                break;
        }
    }

    private void analyzeQueries(String queryString) throws Exception {
        String[] query = queryString.split(" ");
        if (query[1].contains("queries")) {
            countQueries(queryString);
        } else if (query[1].contains("update")) {
            countOperationType("UPDATE");
        } else if (query[1].contains("insert")) {
            countOperationType("INSERT");
        } else if (query[1].contains("delete")) {
            countOperationType("DELETE");
        } else if (query[1].contains("select")) {
            countOperationType("SELECT");
        } else if (query[1].contains("create")) {
            countOperationType("CREATE TABLE");
        }
    }

    private void countOperationType(String operation) throws Exception {
        List<QueryLog> queryLogs = getQueryLogFileInformation();
        HashMap<String, Integer> tableMap = new HashMap<>();
        for (QueryLog queryLog : queryLogs) {
            if (queryLog.getOperation().equalsIgnoreCase(operation)) {
                if (null != tableMap.get(queryLog.getTableName())) {
                    tableMap.put(queryLog.getTableName(), tableMap.get(queryLog.getTableName()) + 1);
                } else {
                    tableMap.put(queryLog.getTableName(), 1);
                }
            }
        }

        Boolean flag = Boolean.FALSE;
        for (Map.Entry<String, Integer> user : tableMap.entrySet()) {
            logger.info("Total " + user.getValue() + " " + operation + " operation(s) are performed on " + user.getKey());
            flag = Boolean.TRUE;
        }

        if (!flag) {
            logger.info("No " + operation + " queries found.");
        }
    }

    private void countQueries(String queryString) throws Exception {
        String [] query = queryString.split(" ");
        Matcher matcher = QueryRegex.countQueriesAnalytics.matcher(queryString);
        if (matcher.find()) {
            String userName = matcher.group(1).trim();
            String databaseName = matcher.group(2).replace(";","");
            countQueriesForUser(userName, databaseName);
        } else if (query.length == 2) {
            countTotalQueries();
        }
    }

    private void countTotalQueries() throws Exception {
        List<QueryLog> queryLogs = getQueryLogFileInformation();
        HashMap<String, Integer> userMap = new HashMap<>();
        for (QueryLog queryLog : queryLogs) {
            String key = queryLog.getSubmittedBy() + "|" + queryLog.getDatabaseName();
            if (null != userMap.get(key)) {
                userMap.put(key, userMap.get(key) + 1);
            } else {
                userMap.put(key, 1);
            }
        }

        Boolean flag = Boolean.FALSE;
        for (Map.Entry<String, Integer> user : userMap.entrySet()) {
            String[] details = user.getKey().split("\\|");
            if (null != details[1] && !details[1].equalsIgnoreCase("null")) {
                logger.info("User " + details[0] + " submitted " + user.getValue() + " query(s) for " + details[1]);
            }
            flag = Boolean.TRUE;
        }

        if (!flag) {
            logger.info("No queries found");
        }
    }

    private void countQueriesForUser(String userName, String databaseName) throws Exception {
        List<QueryLog> queryLogs = getQueryLogFileInformation();
        HashMap<String, Integer> userMap = new HashMap<>();
        for (QueryLog queryLog : queryLogs) {
            if (queryLog.getDatabaseName().equalsIgnoreCase(databaseName) &&
                    queryLog.getSubmittedBy().equalsIgnoreCase(userName)) {
                if (null != userMap.get(queryLog.getSubmittedBy())) {
                    userMap.put(queryLog.getSubmittedBy(), userMap.get(queryLog.getSubmittedBy()) + 1);
                } else {
                    userMap.put(queryLog.getSubmittedBy(), 1);
                }
            }
        }

        Boolean flag = Boolean.FALSE;
        for (Map.Entry<String, Integer> user : userMap.entrySet()) {
            logger.info("User " + user.getKey() + " submitted " + user.getValue() + " query(s) for " + databaseName);
            flag = Boolean.TRUE;
        }

        if (!flag) {
            logger.info("No queries found");
        }
    }

    private List<QueryLog> getQueryLogFileInformation() throws Exception {
        String queryLogFile = FileOperations.readFileContent(
                new File(DataConstants.LOGS_FILE_LOCATION + DataConstants.QUERY_LOG_FILE_NAME));

        String remoteQueryLogFile = RemoteVmUtils.readFileContent(DataConstants.LOGS_FILE_LOCATION + DataConstants.QUERY_LOG_FILE_NAME);
        StringBuilder sb = new StringBuilder()
                .append(queryLogFile)
                .append(remoteQueryLogFile);

        Matcher matcher = QueryRegex.valueBetweenQuotes.matcher(sb.toString());
        List<QueryLog> queryLogList = new ArrayList<>();

        //JSON to Java Object List
        while (matcher.find()) {
            if (matcher.group().equalsIgnoreCase("QueryLog")) {
                QueryLog queryLog = new QueryLog();
                while (matcher.find()) {
                    if (matcher.group().equalsIgnoreCase("flag")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setFlag(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("query")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setQuery(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("operation")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setOperation(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("submissionTimestamp")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setSubmissionTimestamp(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("submittedBy")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setSubmittedBy(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("tableName")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setTableName(matcher.group());
                        }
                    } else if (matcher.group().equalsIgnoreCase("databaseName")) {
                        matcher.find();
                        if (matcher.find()) {
                            queryLog.setDatabaseName(matcher.group());
                        }
                        break;
                    }
                }
                queryLogList.add(queryLog);
            }
        }
        return queryLogList;
    }
}
