package com.dal.distributed.authentication;

import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.authentication.model.UserRegistration;

import java.io.*;
import java.util.Optional;

public class AuthFileUtils {

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

    public static Optional<UserRegistration> readUserDetails(String filePath, String hashedUserId) {
        //TODO get user authentication details from the user details file
        try(FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr)){
            String entireLine;
            while ((entireLine=br.readLine())!=null) {
                String [] userDetailsArr = entireLine.split(MiscConstants.PIPE);
                if (!userDetailsArr[0].equals(hashedUserId))
                    continue;
                // create userRegistration model from
                UserRegistration user = UserRegistration.createUserRegistrationFromString(userDetailsArr);
                return Optional.of(user);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
