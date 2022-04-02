package com.dal.distributed;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.queryImpl.DatabaseCreation;
import com.dal.distributed.queryImpl.TableCreation;

import java.io.IOException;
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

    public void implementQuery(Scanner sc) throws IOException
    {   while (true) {
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
            DatabaseCreation dbCreation= new DatabaseCreation();
            TableCreation tableCreation=new TableCreation();
        switch (userInput) {
            case "1":
                boolean createDbSuccess=dbCreation.createDatabase(query, AuthConstants.DATABASES_FOLDER_LOCATION);
                if(!createDbSuccess)
                System.out.println("Wrong query written");
                break;
            case "2":
                System.out.println(dbCreation.useDatabase(query));
                break;
            case "3":
                System.out.println(tableCreation.createTable(query));
                break;
            case "4":
                break;
            case "5":
                break;
            case "6":
                break;
            case "7":
                break;
            default:
                logger.error("Please choose valid option!");
        }
        if("8".equals(userInput)) {
            logger.info("You are logged out");
            break;
        }
    }
}
}
