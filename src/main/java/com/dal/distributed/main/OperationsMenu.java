package com.dal.distributed.main;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.queryImpl.*;
import com.dal.distributed.queryImpl.model.QueryLog;
import com.dal.distributed.utils.FileOperations;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Scanner;

public class OperationsMenu {

    Logger logger = Logger.instance();

    public void displayOperationsMenu(String userId, Scanner scanner) throws Exception {
        while (true) {
            logger.info("Please choose from the following options:");
            logger.info("\n1. Write Queries");
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
                    break;
                case "3":
                    break;
                case "4":
                    break;
                case "5":
                    break;
                default:
                    logger.error("Please choose valid option!");
            }
            if ("5".equals(userInput)) {
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

        logger.info("Write query for selected option:");
        String query = sc.nextLine();

        QueryLog logQuery = new QueryLog();
        logQuery.setFlag("valid");
        logQuery.setQuery(query);
        logQuery.setSubmissionTimestamp(String.valueOf(new Timestamp(System.currentTimeMillis())));
        logQuery.setSubmittedBy(userId);

        Map queryValidatorResults = queryExecutorObj.validateQuery(query);

        if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.CREATE_DATABASE)) {
            createDatabase.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.USE)) {
            useDatabase.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.CREATE_TABLE)) {
            createTable.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.INSERT)) {
            insertIntoTable.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.SELECT)) {
            selectQuery.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.UPDATE)) {
            updateTable.execute(query);
        } else if (((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType") == QueryTypes.DELETE)) {
            deleteDataFromTable.execute(query);
        } else {
            logQuery.setFlag("invalid");
            logger.error("Oops.. looks like I encountered error in parsing query");
        }
        FileOperations.writeToExistingFile(logQuery.toString(),
                DataConstants.QUERY_LOG_FILE_NAME, DataConstants.QUERY_LOGS_FILE_LOCATION);
    }
}
