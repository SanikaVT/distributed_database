package com.dal.distributed.main;

import com.dal.distributed.analytics.Analytics;
import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.erd.Erd;
import com.dal.distributed.export.ExportDatabase;
import com.dal.distributed.logger.Logger;

import com.dal.distributed.logger.model.EventLog;
import com.dal.distributed.main.model.Pair;
import com.dal.distributed.queryImpl.*;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.queryImpl.model.QueryLog;
import com.dal.distributed.utils.FileOperations;
import transactionProcessing.TransactionProcessing;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class OperationsMenu {
    List<OperationStatus> transactionQueries = new ArrayList<>();
    Logger logger = Logger.instance();

    public void displayOperationsMenu(String userId, Scanner scanner) throws Exception {
        while (true) {
            logger.info("\nPlease choose from the following options:");
            logger.info("1. Write Queries");
            logger.info("2. Generate ERD");
            logger.info("3. Export");
            logger.info("4. Analytics");
            logger.info("5. Logout");
            String userInput = scanner.nextLine();
            switch (userInput) {
                case "1":
                    implementQuery(scanner, userId);
                    break;
                case "2":
                    Erd erd = new Erd();
                    erd.flow(scanner, userId);
                    break;
                case "3":
                    ExportDatabase export = new ExportDatabase();
                    export.flow(scanner);
                    break;
                case "4":
                    Analytics analytics = new Analytics();
                    analytics.analyze(scanner);
                    break;
                case "5":
                    Main.databaseName = "";
                    break;
                default:
                    logger.error("Please choose valid option!");
            }
            if ("5".equals(userInput)) {
                EventLog.logLogoutEvent(userId);
                logger.info("You are logged out");
                break;
            }
        }
    }

    public void implementQuery(Scanner sc, String userId) throws Exception {
        QueryValidator queryExecutorObj = new QueryValidator();
        CreateDatabase createDatabase = new CreateDatabase();
        UseDatabase useDatabase = new UseDatabase();
        CreateTable createTable = new CreateTable();
        InsertIntoTable insertIntoTable = new InsertIntoTable();
        SelectQuery selectQuery = new SelectQuery();
        UpdateTable updateTable = new UpdateTable();
        DeleteDataFromTable deleteDataFromTable = new DeleteDataFromTable();
        do {
            logger.info("Write query for selected option:");
            String query = sc.nextLine();

            QueryLog logQuery = new QueryLog();
            logQuery.setFlag("valid");
            logQuery.setQuery(query);
            logQuery.setSubmissionTimestamp(String.valueOf(new Timestamp(System.currentTimeMillis())));
            logQuery.setSubmittedBy(userId);

            Map queryValidatorResults = queryExecutorObj.validateQuery(query);
            if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.CREATE_DATABASE)) {
                EventLog createDbEvent = new EventLog("CREATE_DB", userId);
                logQuery.setOperation(QueryTypes.CREATE_DATABASE);
                logQuery.setDatabaseName((String) queryValidatorResults.get("entity"));
                Pair<Boolean, String> res = createDatabase.execute(query);
                createDbEvent.setSuccess(res.getFirst());
                createDbEvent.setDatabaseName(res.getSecond());
                if (res.getFirst()) {
                    logger.info("Action: " + query + "\nMessage: 1 row(s) affected.\n");
                }
                EventLog.logEvent(createDbEvent);
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.USE)) {
                logQuery.setOperation(QueryTypes.USE);
                logQuery.setDatabaseName((String) queryValidatorResults.get("entity"));
                if (useDatabase.execute(query)) {
                    logger.info("Action: " + query + "\nMessage: 0 row(s) affected.\n");
                } else {
                    logger.info("Database doesn't exist");
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.CREATE_TABLE)) {
                EventLog createTableEvent = new EventLog("CREATE_TABLE", userId);
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                }
                createTableEvent.setDatabaseName(Main.databaseName);
                logQuery.setOperation(QueryTypes.CREATE_TABLE);
                logQuery.setTableName((String) queryValidatorResults.get("entity"));
                OperationStatus result = createTable.execute(query);
                createTableEvent.setSuccess(result.isStatus());
                createTableEvent.setTableName(logQuery.getTableName());
                if (result.isStatus()) {
                    logger.info("Action: " + query + "\nMessage: 0 row(s) affected.\n");
                }
                logQuery.setDatabaseName(result.getDatabaseName());
                EventLog.logEvent(createTableEvent);
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.INSERT)) {
                logQuery.setOperation(QueryTypes.INSERT);
                logQuery.setTableName((String) queryValidatorResults.get("entity"));
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                } else if (Main.isTransaction) {
                    transactionQueries.add(insertIntoTable.execute(query));
                } else {
                    OperationStatus result = insertIntoTable.execute(query);
                    if (result != null && result.isStatus()) {
                        logger.info("Action: " + query + "\nMessage: 1 row(s) affected.\n");
                        logQuery.setDatabaseName(result.getDatabaseName());
                    } else {
                        logger.error("Required table/columns/data does not exist");
                    }
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.SELECT)) {
                logQuery.setOperation(QueryTypes.SELECT);
                logQuery.setTableName((String) queryValidatorResults.get("entity"));
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                } else if (Main.isTransaction) {
                    transactionQueries.add(selectQuery.execute(query));
                } else {
                    OperationStatus result = selectQuery.execute(query);
                    if (result != null && result.isStatus()) {
                        logQuery.setDatabaseName(result.getDatabaseName());
                    } else {
                        logger.error("Required table/columns/data does not exist");
                    }
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.UPDATE)) {
                logQuery.setOperation(QueryTypes.UPDATE);
                logQuery.setTableName((String) queryValidatorResults.get("entity"));
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                } else if (Main.isTransaction) {
                    transactionQueries.add(updateTable.execute(query));
                } else {
                    OperationStatus result = updateTable.execute(query);
                    if (result != null && result.isStatus()) {
                        logger.info("Action: " + query + "\nMessage: " + result.getCount() + " row(s) affected.\n");
                        logQuery.setDatabaseName(result.getDatabaseName());
                    } else {
                        logger.error("Required table/columns/data does not exist");
                    }
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.DELETE)) {
                logQuery.setOperation(QueryTypes.DELETE);
                logQuery.setTableName((String) queryValidatorResults.get("entity"));
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                } else if (Main.isTransaction) {
                    transactionQueries.add(deleteDataFromTable.execute(query));
                } else {
                    OperationStatus result = deleteDataFromTable.execute(query);
                    if (result != null && result.isStatus()) {
                        logger.info("Action: " + query + "\nMessage: " + result.getCount() + " row(s) affected.\n");
                        logQuery.setDatabaseName(result.getDatabaseName());
                    } else {
                        logger.error("Required table/columns/data does not exist");
                    }
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.START_TRANSACTION)) {
                if (Main.databaseName == null || Main.databaseName.isEmpty()) {
                    logger.error("Please select database first");
                } else {
                    logQuery.setOperation(QueryTypes.START_TRANSACTION);
                    Main.isTransaction = true;
                }
            } else if (((boolean) queryValidatorResults.get("isValidate"))
                    && (queryValidatorResults.get("queryType") == QueryTypes.END_TRANSACTION)) {
                logQuery.setOperation(QueryTypes.END_TRANSACTION);
                TransactionProcessing transactionProcessing = new TransactionProcessing();
                for (int i = 0; i < transactionQueries.size(); i++) {
                    for (int j = i + 1; j < transactionQueries.size(); j++) {
                        if (transactionQueries.get(i).getTableName().equals(transactionQueries.get(j).getTableName())) {
                            transactionQueries.get(j).setRepeatTable(true);
                        }
                    }
                }
                transactionProcessing.execute(transactionQueries);
            } else {
                logQuery.setFlag("invalid");
                logger.error("Oops.. looks like I encountered error in parsing query");
            }
            FileOperations.writeToExistingFile(logQuery.toString(),
                    DataConstants.QUERY_LOG_FILE_NAME, DataConstants.QUERY_LOGS_FILE_LOCATION);
        } while (Main.isTransaction);
    }
}
