package com.dal.distributed.main;

import com.dal.distributed.authentication.Login;
import com.dal.distributed.authentication.Registration;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.miscellaneous.MiscOperations;
import java.util.Scanner;

public class Main {
    public static String databaseName="";

    public static void main(String [] args) throws Exception {
        Logger logger = Logger.instance();
        MiscOperations.createInitFolders();
        logger.info("Welcome to DPG9 Distributed Database");
        while (true) {
            logger.info("\n1. User Registration");
            logger.info("2. Login");
            logger.info("3. Exit\n");
            logger.info("Please select an option from the above list");
            Scanner sc = new Scanner(System.in);
            //new OperationsMenu().implementQuery(sc);
            final String userInput = sc.nextLine();
            switch (userInput) {
                case "1":
                    Registration registration = new Registration();
                    registration.registerUser();
                    break;
                case "2":
                    Login login = new Login();
                    login.flow(sc);
                    break;
                case "3":
                    break;
                default:
                    logger.error("Please enter a valid input.");
            }
            if (userInput.equals("3")) {
                break;
            }
        }
    }
}
