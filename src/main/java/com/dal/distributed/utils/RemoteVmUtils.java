package com.dal.distributed.utils;

import com.dal.distributed.constant.MiscConstants;
import com.dal.distributed.constant.VMConstants;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.util.*;

public class RemoteVmUtils {

    /**
     *
     * @param command
     * @return
     * @throws Exception
     */
    public static String getOutput(String command) throws Exception {
        Session session = null;
        ChannelExec channel = null;
        String responseString = null;
        try {
            JSch jSch = new JSch();
            jSch.addIdentity(VMConstants.PRIVATE_KEY);
            session = jSch.getSession(VMConstants.USERNAME, VMConstants.EXTERNAL_IP, VMConstants.port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
            channel.setOutputStream(responseStream);
            channel.connect();

            while (channel.isConnected()) {
                Thread.sleep(100);
            }

            responseString = new String(responseStream.toByteArray());
        } finally {
            if (session != null) {
                session.disconnect();
            }
            if (channel != null) {
                channel.disconnect();
            }
        }
        return responseString;
    }

    /**
     *
     * @param command
     * @return
     * @throws Exception
     */
    public static String runCommand(String command) throws Exception {
        Session session = null;
        ChannelExec channel = null;
        String responseString = null;
        try {
            JSch jSch = new JSch();
            jSch.addIdentity(VMConstants.PRIVATE_KEY);
            session = jSch.getSession(VMConstants.USERNAME, VMConstants.EXTERNAL_IP, VMConstants.port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
            channel.setOutputStream(responseStream);
            channel.connect();

            while (channel.isConnected()) {
                Thread.sleep(100);
            }

            responseString = new String(responseStream.toByteArray());
            System.out.println(responseString);
        } finally {
            if (session != null) {
                session.disconnect();
            }
            if (channel != null) {
                channel.disconnect();
            }
        }
        return responseString;
    }

    /**
     *
     * @param dir
     * @return
     * {
     *     "files" : [],
     *     "folders" : []
     * }
     * eg: {folders=[chanpreet], files=[a.txt, b.txt, c.txt]}
     * @throws Exception
     */
    public static Map<String, ArrayList> readFiles(String dir) throws Exception {
        dir = VMConstants.projectPath + dir;
        String commandResult = getOutput("ls -al " + dir);
        ArrayList folders = new ArrayList();
        ArrayList files = new ArrayList();
        Map result = new HashMap(){{
            put("files", files);
            put("folders", folders);
        }};
        if(commandResult!=null){
            String[] outputLines = commandResult.split("\\n");
            for(String eachLine:outputLines){
                if((eachLine.startsWith("d")) || (eachLine.startsWith("-"))){
                    String entityName = extractNameFromLS(eachLine);
                    if((!entityName.contentEquals(".")) && (!entityName.contentEquals(".."))){
                        if(eachLine.startsWith("d"))
                            folders.add(entityName);
                        else
                            files.add(entityName);
                    }
                }
            }
            if(!files.isEmpty())
                result.replace("files", files);
            if(!folders.isEmpty())
                result.replace("folders", folders);
        }
        return result;
    }

    /**
     *
     * @param text
     * @return
     */
    public static String extractNameFromLS(String text){
        String[] splittedText = text.split("\\s");
        return splittedText[splittedText.length - 1];
    }

    /**
     *
     * @param filePath
     * @return
     * @throws Exception
     */
    public static String readFileContent(String filePath) throws Exception {
        return getOutput("cat " + filePath);
    }

    /**
     *
     * @param fileContent
     * @param filename
     * @param fileDirectory
     * @throws Exception
     */
    public static void writeToExistingFile(String fileContent, String filename, String fileDirectory) throws Exception {
        fileDirectory = VMConstants.projectPath + fileDirectory;
        String command = "echo \"" + fileContent + "\" >> " + fileDirectory + filename;
        runCommand(command);
    }

    /**
     *
     * @param filepath
     * @param folderName
     * @return
     * @throws Exception
     */
    public static boolean createNewFolder(String filepath, String folderName) throws Exception {
        filepath = VMConstants.projectPath + filepath;
        StringBuilder sb = new StringBuilder();
        if (filepath != null)
            sb.append(filepath).append("/");
        if (folderName != null)
            sb.append(folderName);
        runCommand("mkdir " + sb.toString());
        return true;
    }

    /**
     * @param text
     * @return
     */
    public static ArrayList getArrayForPipeString(String text) {
        if (text != null){
            ArrayList<String> result = new ArrayList();
            String[] splittedText = text.split(MiscConstants.PIPE);
            for(String each:splittedText){
                each = each.trim();
                result.add(each);
            }
            return result;
        }

        else
            return null;
    }

    /**
     * @param filePath
     * @return ArrayList - first element is {"columns" : []}, second onwards - Map<>
     * @throws Exception
     */
    public static ArrayList<Map<String, Object>> readPsvFileForQueryOps(String filePath) throws Exception {
        filePath = VMConstants.projectPath + filePath;
        ArrayList result = new ArrayList();
        ArrayList columns = new ArrayList();
        String fileContent = readFileContent(filePath);
        String[] lines = fileContent.split("\\n");
        int count = 0;
        while (count< lines.length) {
            if (count == 0) {
                columns = getArrayForPipeString(lines[count]);
                if (columns == null)
                    break;
                else {
                    ArrayList finalCols = columns;
                    Map dataDict = new HashMap() {{
                        put("columns", finalCols);
                    }};
                    result.add(dataDict);
                    count++;
                }
            } else {
                if (columns.size() == 0)
                    break;
                else {
                    ArrayList rowData = getArrayForPipeString(lines[count]);
                    if (rowData != null) {
                        Map dataDict = new HashMap<String, Object>();
                        for (int i = 0; i < columns.size(); i++) {
                            dataDict.put(columns.get(i), rowData.get(i));
                        }
                        result.add(dataDict);
                    }
                }
            }
            count++;
        }
        return result;
    }

    /**
     *
     * @param filePath
     * @return
     * @throws Exception
     */
    public static List<List<Object>> readDataFromPSV(String filePath) throws Exception {
        filePath = VMConstants.projectPath + filePath;
        List<List<Object>> rows = new ArrayList<>();
        List<Object> columnValues;

        String path;
        if (filePath.contains(".psv")) {
            path = filePath;
        } else {
            path = filePath + ".psv";
        }
        String fileContent = readFileContent(filePath);
        System.out.println("RMUtils: " + fileContent);
        String[] lines = fileContent.split("\\n");
        System.out.println("Lines Length: " + lines.length);
        int count = 0;
        while(count<lines.length){
            String line = lines[count];
            columnValues = new ArrayList<>(Arrays.asList(line.split(MiscConstants.PIPE, -1)));
            rows.add(columnValues);
            count++;
        }
        return rows;
    }

    /**
     *
     * @param rows
     * @param filePath
     */
    public void writeDataToPSV(List<List<Object>> rows, String filePath) {
        filePath = VMConstants.projectPath + filePath;
        StringBuilder sb = new StringBuilder();
        try {
            filePath = filePath + ".psv";
            for (List<Object> rowData : rows)
                sb.append(rowData.toString().replace("[", "").replace("]", "").replaceAll(",", "|")).append("\n");
            runCommand("echo \"" + sb.toString() + "\" " + filePath);
        } catch (Exception e) {
            e.getCause();
        }
    }

    /**
     *
     * @param row
     * @param filePath
     * @throws Exception
     */
    public static void writeStringToPSV(String row, String filePath) throws Exception {
        writeToExistingFile(row, null, filePath);
    }
}