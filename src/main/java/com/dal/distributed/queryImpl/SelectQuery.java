package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.RelationalOperators;
import com.dal.distributed.utils.DataUtils;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.logger.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

public class SelectQuery {
    Logger logger = Logger.instance();
    public void execute(String query) throws Exception {
        ArrayList resultList = new ArrayList();
        Map resultDict;
        Matcher matcher = QueryRegex.selectDataFromTable.matcher(query);
        if(matcher.find()){
            String tableName = matcher.group(5);
//        Query to test: select * from Persons2;
//        Uncomment the line when not testing
//            ArrayList fileContent = FileOperations.readPsvFileForQueryOps(DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/" + table + DataConstants.FILE_FORMAT);
            ArrayList fileContent = FileOperations.readPsvFileForQueryOps(DataConstants.DATABASES_FOLDER_LOCATION + "dbdbdb/" + tableName + DataConstants.FILE_FORMAT);
            String projections = matcher.group(1);
            if(projections == null)
                logger.info("Oops.. looks like you did not add projections! Please try again!");
            else{
                ArrayList projectionList = new ArrayList();
                projections = projections.trim();
                if(projections.contentEquals("*")){
                    Map x = (Map) fileContent.get(0);
                    projectionList = (ArrayList) x.get("columns");
                }

                else {
                    String[] x = projections.split(MiscConstants.PIPE);
                    for (String each : x) {
                        each = each.trim();
                        projectionList.add(each);
                    }
                }
                String compareColumn=null, relationOperator=null, value=null, valueType=null;
                compareColumn = matcher.group(7);
                relationOperator = matcher.group(8);
                value = matcher.group(9);
                if(value != null){
                    value = value.trim();
                    valueType = DataUtils.getDataType(value);
                    if(valueType=="String"){
                        value = value.substring(value.indexOf("'")+1, value.lastIndexOf("'"));
                        value = value.trim();
                    }
                }
                if((relationOperator != null) && (compareColumn != null) && (value != null)){
                    switch (relationOperator){
                        case RelationalOperators.EQUAL:
                            for(int i=1; i<fileContent.size(); i++){
                                Map dataDict = (Map) fileContent.get(i);
                                if((dataDict.containsKey(compareColumn)) && (dataDict.get(compareColumn).equals(value)))
                                    resultList.add(dataDict);
                            }
                            break;
                        case RelationalOperators.GREATER:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && ((int) dataDict.get(compareColumn) > Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.GREATER + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.LESS:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && ((int) dataDict.get(compareColumn) < Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.LESS + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.GREATEREQUAL:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && ((int) dataDict.get(compareColumn) >= Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.GREATEREQUAL + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.LESSEQUAL:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && ((int) dataDict.get(compareColumn) >= Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.LESSEQUAL + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.NOTEQUAL:
                        case RelationalOperators.NOTEQUAL1:
                        case RelationalOperators.NOTEQUAL2:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && ((int) dataDict.get(compareColumn) != Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else{
                                    if ((dataDict.containsKey(compareColumn)) && (!dataDict.get(compareColumn).equals(value)))
                                        resultList.add(dataDict);
                                }
                            }
                            break;
                        default:
                    }
                }
                else{
                    for(int i=1; i<fileContent.size(); i++) {
                        Map dataDict = (Map) fileContent.get(i);
                        resultList.add(dataDict);
                    }
                }
                resultList = this.filterProjectionsForOutput(resultList, projectionList);
            }
        }
    }

    private ArrayList filterProjectionsForOutput(ArrayList resultList, ArrayList projectionList) {
        ArrayList result = new ArrayList();
        for(int i=0; i<resultList.size(); i++){
            Map dataDict = (Map) resultList.get(i);
            Map resultDict = new HashMap();
            for(Object eachKey:dataDict.keySet()){
                if(projectionList.contains((String) eachKey))
                    resultDict.put((String) eachKey, dataDict.get((String) eachKey));
            }
            if(resultDict!=null)
                result.add(resultDict);
        }
        return result;
    }
}