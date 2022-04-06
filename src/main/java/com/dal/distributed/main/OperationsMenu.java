package com.dal.distributed.main;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.logger.Logger;

import com.dal.distributed.queryImpl.*;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class OperationsMenu {

    Logger logger = Logger.instance();

    public void displayOperationsMenu(String userId, Scanner scanner) throws IOException {
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
                    implementQuery(scanner);
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
            if("5".equals(userInput)) {
                logger.info("You are logged out");
                break;
            }
        }
    }

    public void implementQuery(Scanner sc) throws IOException {
        QueryValidator queryExecutorObj = new QueryValidator();
        CreateDatabase createDatabase = new CreateDatabase();
        UseDatabase useDatabase = new UseDatabase();
        CreateTable createTable = new CreateTable();
        InsertIntoTable insertIntoTable = new InsertIntoTable();
        SelectQuery selectQuery = new SelectQuery();
        UpdateTable updateTable = new UpdateTable();
        DeleteDataFromTable deleteDataFromTable = new DeleteDataFromTable();

        Map queryValidatorResults;
        while (true) {
        String query="";
            logger.info("Please choose from the following options:");
            logger.info("\n1. Create databases");
            logger.info("2. Use databases");
            logger.info("3. Create table");
            logger.info("4. Insert into table");
            logger.info("5. Select from table with single where condition");
            logger.info("6. Update one column with single where condition");
            logger.info("7. Delete a row with single where condition");
            logger.info("8. Exit");
            String userInput = sc.nextLine();
            if(!userInput.equals("8")){
            logger.info("Write query for selected option:");
            query=sc.nextLine();
            }

        switch (userInput) {
            case "1":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.CREATE_DATABASE)){
                    createDatabase.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "2":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.USE)){
                    useDatabase.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "3":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.CREATE_TABLE)){
                    createTable.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "4":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.INSERT)){
                    insertIntoTable.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "5":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.SELECT)){
                    selectQuery.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "6":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.UPDATE)){
                    updateTable.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            case "7":
                queryValidatorResults = queryExecutorObj.validateQuery(query);
                if(((boolean) queryValidatorResults.get("isValidate")) && (queryValidatorResults.get("queryType")== QueryTypes.DELETE)){
                    deleteDataFromTable.execute(query);
                }
                else
                    logger.error("Oops.. looks like I encountered error in parsing query");
                break;
            default:
                logger.error("Please choose valid option!");
        }
        if("8".equals(userInput)) {
            logger.info("You are logged out.");
            break;
        }
    }
}
}
