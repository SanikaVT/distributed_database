package com.dal.distributed.main;

import com.dal.distributed.authentication.Login;
import com.dal.distributed.authentication.Registration;
import com.dal.distributed.logger.Logger;

import java.util.Scanner;

public class Main {

    public static void main(String [] args) {
        Logger logger = Logger.instance();

        logger.info("Welcome to DPG9 Distributed Database\n");
        logger.info("Please select an option from the below list");
        logger.info("1. User Registration");
        logger.info("2. Login");
        logger.info("3. Exit");

        Scanner sc = new Scanner(System.in);
        final String userInput = sc.nextLine();

        Boolean flag = Boolean.FALSE;

        while (true) {
            switch (userInput) {
                case "1":
                    Registration registration = new Registration();
                    registration.registerUser();
                    break;
                case "2":
                    Login login = new Login();
                    login.accessSystem();
                    break;
                case "3":
                    flag = Boolean.TRUE;
                    break;
                default:
                    logger.error("Please enter a valid input.");
            }
            if (flag == Boolean.TRUE) {
                break;
            }
        }
    }
}
