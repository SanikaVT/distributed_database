package com.dal.distributed;

import com.dal.distributed.logger.Logger;

import java.util.Scanner;

public class OperationsMenu {

    Logger logger = Logger.instance();

    public void displayOperationsMenu(String userId, Scanner scanner) {
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
}
