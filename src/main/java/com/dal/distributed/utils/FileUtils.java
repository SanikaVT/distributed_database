package com.dal.distributed.utils;

import com.dal.distributed.model.UserRegistration;

public class FileUtils {

    public void writeUserDetails(String filePath, UserRegistration userDetails){
        //TODO write user authentication details to the user details file
    }

    public UserRegistration readUserDetails(String filePath, String userId) {
        //TODO get user authentication details from the user details file
        return new UserRegistration();
    }
}
