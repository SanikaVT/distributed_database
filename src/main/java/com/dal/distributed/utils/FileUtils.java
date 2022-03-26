package com.dal.distributed.utils;

import com.dal.distributed.logger.Logger;
import com.dal.distributed.model.UserRegistration;

import java.io.FileWriter;
import java.io.IOException;

public class FileUtils {

    Logger logger = Logger.instance();

    /**
     * This method writes the user registration information in the file
     *
     * @param filePath            file path
     * @param registrationDetails user input
     */
    public void writeUserDetails(String filePath, String registrationDetails) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(filePath, true);
            fileWriter.write(registrationDetails);
            fileWriter.write("\n");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public UserRegistration readUserDetails(String filePath, String userId) {
        //TODO get user authentication details from the user details file
        return new UserRegistration();
    }
}
