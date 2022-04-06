package com.dal.distributed.authentication;

import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.authentication.model.SecurityQuestions;
import com.dal.distributed.authentication.model.UserRegistration;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Pattern;

public class Registration {

    private static final String PASSWORD_VALIDATION_REGEX = "^[a-zA-Z0-9!@#$&*]{8,20}$";

    private static final String USER_ID_VALIDATION_REGEX = "^[a-zA-Z0-9]{5,15}";

    private final String SECURITY_ANS_VALIDATION_REGEX = "^[a-zA-Z0-9 !@#$%^&*]{3,20}$";

    Logger logger = Logger.instance();

    public void registerUser() {
        Scanner sc = new Scanner(System.in);

        String userId;
        String password;

        logger.info("Enter a UserID containing 5 to 15 characters: ");
        userId = sc.nextLine();
        Boolean userIdValid = performUserIdValidations(userId);
        if (!userIdValid) {
            return;
        }

        logger.info("Enter a password");
        password = sc.nextLine();
        Boolean isPasswordValid = performPasswordValidations(password);
        if (!isPasswordValid) {
            return;
        }

        logger.info(AuthConstants.SECURITY_QUESTION_1);
        final String securityAnswerOne = sc.nextLine();
        if (!validateSecurityInput(securityAnswerOne)) {
            return;
        }

        logger.info(AuthConstants.SECURITY_QUESTION_2);
        final String securityAnswerTwo = sc.nextLine();
        if (!validateSecurityInput(securityAnswerTwo)) {
            return;
        }

        logger.info(AuthConstants.SECURITY_QUESTION_3);
        final String securityAnswerThree = sc.nextLine();
        if (!validateSecurityInput(securityAnswerThree)) {
            return;
        }

        List<SecurityQuestions> securityQuestions = new ArrayList<>();
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_1, securityAnswerOne));
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_2, securityAnswerTwo));
        securityQuestions.add(new SecurityQuestions(AuthConstants.SECURITY_QUESTION_3, securityAnswerThree));

        //Prepare the user registration object to be written in the User_Profile
        UserRegistration user = new UserRegistration();
        user.setUserId(hash(userId));
        user.setPassword(hash(password));
        user.setSecurityQuestions(securityQuestions);

        AuthFileUtils file = new AuthFileUtils();
        file.writeUserDetails(AuthConstants.USER_DETAILS_FILE_LOCATION, user.toString());

        logger.info("Registration completed successfully!!! You can now access the system with userID and Password.");
    }

    /**
     * This method returns hashed password that is hashed using MD5
     *
     * @param password Input
     * @return Hashed Password String
     */
    private String hash(String password) {
        return DigestUtils.md5Hex(password);
    }

    /**
     * This method performs userID validations
     *
     * @param userId Input
     * @return True if the validations pass and false if fail
     */
    private Boolean performUserIdValidations(String userId) {
        Boolean isValid = Boolean.TRUE;
        if (userId.isEmpty() || userId.trim().isEmpty()) {
            isValid = Boolean.FALSE;
        } else {
            if (!checkIfTheUserIdExists(userId)) {
                if (!validateUserId(userId)) {
                    isValid = Boolean.FALSE;
                }
            } else {
                logger.error("UserID already exists. Please enter a new userID.");
                isValid = Boolean.FALSE;
            }
        }
        return isValid;
    }

    /**
     * This method checks if the userID already exists in the system
     *
     * @param userId Input
     * @return True if exists and False if does not exist
     */
    private boolean checkIfTheUserIdExists(String userId) {
        String hashedUserId = hash(userId);
        Optional<UserRegistration> userOpt = AuthFileUtils.readUserDetails(AuthConstants.USER_DETAILS_FILE_LOCATION, hashedUserId);
        return userOpt.isPresent();
    }

    /**
     * This method validates userID entered by the user.
     * Condition: Alphanumeric, length should be between 5 and 15
     *
     * @param userId Input
     * @return True if valid and False if invalid
     */
    private Boolean validateUserId(String userId) {
        Pattern pattern = Pattern.compile(USER_ID_VALIDATION_REGEX);
        if (!pattern.matcher(userId).matches()) {
            logger.error("Please enter a valid alphanumeric userID. Accepted Length: 5 to 15");
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * This method validates the password entered by the user
     *
     * @param password Input
     * @return True if valid and False if invalid
     */
    private Boolean performPasswordValidations(String password) {
        Boolean isValid = Boolean.TRUE;
        if (password.isEmpty() || password.trim().isEmpty()) {
            isValid = Boolean.FALSE;
        } else {
            Pattern pattern = Pattern.compile(PASSWORD_VALIDATION_REGEX);
            if (!pattern.matcher(password).matches()) {
                logger.error("Please enter a valid password. Accepted Length: 8 to 20 | Allowed special characters: ! @ # $ & *");
                return Boolean.FALSE;
            }
        }
        return isValid;
    }

    /**
     * This method validates the user input for security questions.
     *
     * @param securityAnswer Input
     * @return True if valid and False if invalid
     */
    private boolean validateSecurityInput(String securityAnswer) {
        if (securityAnswer.isEmpty() || securityAnswer.trim().isEmpty()) {
            logger.error("Please enter a non empty response.");
            return Boolean.FALSE;
        } else {
            Pattern pattern = Pattern.compile(SECURITY_ANS_VALIDATION_REGEX);
            if (!pattern.matcher(securityAnswer).matches()) {
                logger.error("Please enter a valid answer. Accepted Length: 3 to 20");
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }
    }
}
