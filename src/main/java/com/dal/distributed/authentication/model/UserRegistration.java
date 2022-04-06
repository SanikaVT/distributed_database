package com.dal.distributed.authentication.model;


import com.dal.distributed.constant.AuthConstants;

import java.util.ArrayList;
import java.util.List;

public class UserRegistration {
    private String userId;
    private String password;
    private List<SecurityQuestions> securityQuestions;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<SecurityQuestions> getSecurityQuestions() {
        return securityQuestions;
    }

    public void setSecurityQuestions(List<SecurityQuestions> securityQuestions) {
        this.securityQuestions = securityQuestions;
    }

    @Override
    public String toString() {
        StringBuilder userRegistration = new StringBuilder(200);
        userRegistration
                .append(userId)
                .append("|")
                .append(password)
                .append("|");

        for (SecurityQuestions question : securityQuestions) {
            userRegistration
                    .append(question.getAnswer())
                    .append("|");
        }
        //Delete the last appended pipe
        userRegistration.deleteCharAt(userRegistration.length() - 1);
        return userRegistration.toString();
    }

    /**
     * This function is used to create UserRegistration object
     * from the User_Details files which is stored in pipe separated
     * format.
     * @param userDetails
     * @return UserRegistration object with all the details along with security questions
     */
    public static UserRegistration createUserRegistrationFromString(String [] userDetails) {
        if(userDetails.length != 5)
            throw new IllegalArgumentException("Doesn't have enough user details");
        UserRegistration user = new UserRegistration();
        user.setUserId(userDetails[0]);
        user.setPassword(userDetails[1]);
        List<SecurityQuestions> securityQuestions = new ArrayList<>(3);
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_1, userDetails[2]));
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_2, userDetails[3]));
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_3, userDetails[4]));
        user.setSecurityQuestions(securityQuestions);
        return user;
    }
}
