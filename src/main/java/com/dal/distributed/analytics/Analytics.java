package com.dal.distributed.analytics;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.queryImpl.model.QueryLog;
import com.dal.distributed.utils.FileOperations;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;

public class Analytics {
    Logger logger = Logger.instance();

    public void analyze(Scanner scanner) throws IOException {
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

    private void analyzeQueries(String queryString) throws IOException {
        String [] query = queryString.split(" ");
        if (query[1].equalsIgnoreCase("queries")) {
            countQueries();
        } else if (query[1].equalsIgnoreCase("update")) {
            countOperationType("UPDATE");
        } else if (query[1].equalsIgnoreCase("insert")) {
            countOperationType("INSERT");
        } else if (query[1].equalsIgnoreCase("delete")) {
            countOperationType("DELETE");
        } else if (query[1].equalsIgnoreCase("update")) {
            countOperationType("UPDATE");
        } else if (query[1].equalsIgnoreCase("select")) {
            countOperationType("SELECT");
        }
    }

    private void countOperationType(String operation) throws IOException {
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

    private void countQueries() throws IOException {
        List<QueryLog> queryLogs = getQueryLogFileInformation();
        HashMap<String, Integer> userMap = new HashMap<>();
        for (QueryLog queryLog : queryLogs) {
            if (null != userMap.get(queryLog.getSubmittedBy())) {
                userMap.put(queryLog.getSubmittedBy(), userMap.get(queryLog.getSubmittedBy()) + 1);
            } else {
                userMap.put(queryLog.getSubmittedBy(), 1);
            }
        }

        Boolean flag = Boolean.FALSE;
        for (Map.Entry<String, Integer> user : userMap.entrySet()) {
            logger.info("User " + user.getKey() + " submitted " + user.getValue() + " query(s).");
            flag = Boolean.TRUE;
        }

        if (!flag) {
            logger.info("No queries found");
        }
    }

    private List<QueryLog> getQueryLogFileInformation() throws IOException {
        String queryLogFile = FileOperations.readFileContent(
                new File(DataConstants.LOGS_FILE_LOCATION + DataConstants.QUERY_LOG_FILE_NAME));
        Matcher matcher = QueryRegex.valueBetweenQuotes.matcher(queryLogFile);
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
                        break;
                    }
                }
                queryLogList.add(queryLog);
            }
        }
        return queryLogList;
    }
}
