package com.dal.distributed.authentication;

import com.dal.distributed.OperationsMenu;
import com.dal.distributed.constant.AuthConstants;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.model.SecurityQuestions;
import com.dal.distributed.model.UserRegistration;
import com.dal.distributed.utils.FileUtils;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Login {

    public static Logger logger = Logger.instance();

    public void flow(Scanner sc) {
        logger.info("For login, please provide your userId and press enter");
        String userId = sc.nextLine();
        if(userId == null || userId.isEmpty()) {
            logger.info("Please type something before enter!");
            return;
        }
        logger.info("Please provide your password and press enter");
        String password = sc.nextLine();
        if(password == null || password.isEmpty()) {
            logger.error("Password can't be empty!");
            return;
        }
        Optional<UserRegistration> userOpt = FileUtils.readUserDetails(AuthConstants.USER_DETAILS_FILE_LOCATION, getHashedValue(userId));
        if(!userOpt.isPresent()) {
            logger.error("Either userId/password is not correct");
            return;
        }
        UserRegistration user = userOpt.get();
        String hashedPassword = getHashedValue(password);
        if(!hashedPassword.equals(user.getPassword())) {
            logger.error("Either userId/password is not correct");
            return;
        }
        int randomQuestionNumber = ThreadLocalRandom.current().nextInt(0, 3);
        SecurityQuestions securityQuestion = user.getSecurityQuestions().get(randomQuestionNumber);
        logger.info("Please answer following security question and press enter");
        logger.info(securityQuestion.getQuestion());
        String securingAnsByUser = sc.nextLine();
        if(securingAnsByUser == null || securingAnsByUser.isEmpty()) {
            logger.error("Your security answer can't be empty!");
            return;
        }
        if(!securingAnsByUser.equals(securityQuestion.getAnswer())) {
            logger.info("Invalid answer please try again!");
            return;
        }
        logger.info("You are successfully logged in!!");
        OperationsMenu operationsMenu = new OperationsMenu();
        operationsMenu.displayOperationsMenu(userId, sc);
        return ;
    }

    private String getHashedValue(String originalString) {
        return DigestUtils.md5Hex(originalString);
    }
}
