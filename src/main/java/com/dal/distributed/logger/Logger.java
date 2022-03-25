package com.dal.distributed.logger;

public class Logger {

    private static Logger instance = null;

    private Logger() {

    }

    /**
     * This method returns the object of PrintMessage.
     *
     * @return PrintMessage singleton object
     */
    public static Logger instance() {
        if (null == instance) {
            return new Logger();
        } else {
            return instance;
        }
    }

    public void info(String message) {
        System.out.println(message);
        //To Do: Write to info log file
    }

    public void error(String message) {
        System.err.println(message);
        //To Do: Write to error log file
    }
}
