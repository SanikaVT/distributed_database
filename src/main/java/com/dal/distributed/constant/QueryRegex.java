package com.dal.distributed.constant;

import java.util.regex.Pattern;

public class QueryRegex {
    public static final Pattern createDatabase = Pattern.compile("create\\s+database\\s+(\\w+)\\;", Pattern.CASE_INSENSITIVE);
    public static final Pattern useDatabase = Pattern.compile("USE\\s+(\\w+)\\;", Pattern.CASE_INSENSITIVE);
    public static final Pattern createTable = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.+)\\)\\;", Pattern.CASE_INSENSITIVE);
    public static final Pattern updateTable = Pattern.compile("UPDATE\\s+(\\w+)\\s+SET\\s+(\\w+\\=(\\w|\\'\\'|\\'\\w+\\'))\\s*(WHERE\\s+(\\w+\\s*(\\=|\\<|\\>|\\<\\=|\\>\\=)\\s*(\\'\\w*\\'|\\w+)))?\\;", Pattern.CASE_INSENSITIVE);
    public static final Pattern deleteDataInTable = Pattern.compile("DELETE\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+\\s*(\\=|\\<|\\>|\\<\\=|\\>\\=)\\s*(\\'\\w*\\'|\\w+))\\;\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern insertDataIntoTable = Pattern.compile("insert\\s+into\\s+(\\w+)((.+))?\\s+values\\s*\\((.+)\\);\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern selectDataFromTable = Pattern.compile("select\\s+((\\*)|(\\w+)|(\\w+\\,\\s*)+\\w+)\\s+from\\s+(\\w+)\\s*(where\\s+(\\w+)\\s*(\\=|\\<|\\>|\\!\\=|\\<\\=|\\>\\=)\\s*(\\d+|\\'\\w+\\'))?\\;$", Pattern.CASE_INSENSITIVE);
    public static final Pattern startTransaction = Pattern.compile("^start\\s+transaction;\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern endTransaction = Pattern.compile("^end\\s+transaction;\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern valueBetweenQuotes = Pattern.compile("(?<=\").*?(?=\")");
    public static final Pattern countQueriesAnalytics = Pattern.compile("count\\s+queries\\s+by\\s+(.*)for\\s+(.*)");

    public static final Pattern digitOnlyRegex = Pattern.compile("^[0-9]+$");

}