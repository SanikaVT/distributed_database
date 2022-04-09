package com.dal.distributed.queryImpl;

import com.dal.distributed.constant.DataConstants;
import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.constant.QueryRegex;
import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.constant.RelationalOperators;
import com.dal.distributed.utils.DataUtils;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.Results;
import com.dal.distributed.logger.Logger;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.model.OperationStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import javax.naming.spi.DirStateFactory.Result;

public class SelectQuery {
    Logger logger = Logger.instance();
    public OperationStatus execute(String query) throws Exception {
        if(Main.databaseName==null){
            System.out.println("No database selected");
            return null;
        }
        OperationStatus operationStatus=null;
        List<Map> resultList = new ArrayList();
        List<List<Object>> queryResult=new ArrayList<>();
        Map resultDict;
        Matcher matcher = QueryRegex.selectDataFromTable.matcher(query);
        if(matcher.find()){
            String tableName = matcher.group(5);
//        Query to test: select * from Persons2;
//        Uncomment the line when not testing
            String filePath=DataConstants.DATABASES_FOLDER_LOCATION + Main.databaseName + "/" + tableName + DataConstants.FILE_FORMAT;
          ArrayList fileContent = FileOperations.readPsvFileForQueryOps(filePath);
            //ArrayList fileContent = FileOperations.readPsvFileForQueryOps(DataConstants.DATABASES_FOLDER_LOCATION + "dbdbdb/" + tableName + DataConstants.FILE_FORMAT);
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
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if((dataDict.containsKey(compareColumn)) && (dataDict.get(compareColumn).equals(value)))
                                    resultList.add(dataDict);
                            }
                            break;
                        case RelationalOperators.GREATER:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && (Integer.parseInt(dataDict.get(compareColumn)) > Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.GREATER + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.LESS:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && (Integer.parseInt(dataDict.get(compareColumn)) < Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.LESS + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.GREATEREQUAL:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && (Integer.parseInt(dataDict.get(compareColumn)) >= Integer.parseInt(value)))
                                        resultList.add(dataDict);
                                }
                                else
                                    logger.error("I cannot apply " + RelationalOperators.GREATEREQUAL + " on datatypes other than int!");
                            }
                            break;
                        case RelationalOperators.LESSEQUAL:
                            for(int i=1; i<fileContent.size(); i++) {
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && (Integer.parseInt(dataDict.get(compareColumn) )>= Integer.parseInt(value)))
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
                                Map<String,String> dataDict = (Map) fileContent.get(i);
                                if (valueType == "int") {
                                    if ((dataDict.containsKey(compareColumn)) && (Integer.parseInt(dataDict.get(compareColumn)) != Integer.parseInt(value)))
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
                        Map<String,String> dataDict = (Map) fileContent.get(i);
                        resultList.add(dataDict);
                    }
                }
                List<Object> mapToList=new ArrayList<>();
                resultList = this.filterProjectionsForOutput(resultList, projectionList);
                int c=0;
                for(Map<String,String> m:resultList){
                    if(c==0)
                    {
                    for(Map.Entry<String,String> mp : m.entrySet())
                    {
                        mapToList.add(mp.getKey());
                    }
                    queryResult.add(mapToList);
                }
                mapToList=new ArrayList<>();
                    c+=1;
                    for(Map.Entry<String,String> mp : m.entrySet())
                    {
                        mapToList.add(mp.getValue());
                    }
                    queryResult.add(mapToList);
                }
                operationStatus=new OperationStatus(true, queryResult, query, filePath, QueryTypes.SELECT, tableName);
                if(!Main.isTransaction)
                Results.printResult(queryResult);
            }
        }
        return operationStatus;
    }

    private ArrayList filterProjectionsForOutput(List<Map> resultList, ArrayList projectionList) {
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