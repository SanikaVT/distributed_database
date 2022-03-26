package com.dal.distributed.model;

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
}
