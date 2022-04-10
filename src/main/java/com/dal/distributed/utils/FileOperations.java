package com.dal.distributed.utils;
import com.dal.distributed.constant.MiscConstants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;

import com.dal.distributed.constant.MiscConstants;

import java.io.*;
import java.util.*;


// Class for file operations
public class FileOperations {
    static PrintWriter printWriter;
    // Read files from the directory

    public static List<String> getColumnDefinitions(File table) {
        try (FileReader fr = new FileReader(table);
             BufferedReader br = new BufferedReader(fr)){
            String columnDefLine = br.readLine();
            return Arrays.asList(columnDefLine.split(MiscConstants.PIPE));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * @param dir
     * @return
     */
    public static File[] readFiles(String dir) {
        File file = new File(dir);
        return file.listFiles();
    }

    //Read file content from specific file, returns String content

    /**
     * @param file
     * @return
     * @throws IOException
     */
    public static String readFileContent(File file) throws IOException {
        StringBuilder jString = new StringBuilder();
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            jString.append(scanner.nextLine());
            jString.append(System.lineSeparator());
        }
        scanner.close();
        return jString.toString();

    }

    //Write file to the given directory

    /**
     * @param fileContent
     * @param filename
     * @param fileDirectory
     * @throws FileNotFoundException
     */
    public static void writeToNewFile(String fileContent, String filename, String fileDirectory) throws FileNotFoundException {

        try {
            printWriter = new PrintWriter(fileDirectory + filename);
            printWriter.write(fileContent);
            printWriter.flush();
            printWriter.close();
        } catch (FileNotFoundException f) {
            System.out.println("FileNotFound: Change path of the directory in IProperties.class");
        }
    }

    /**
     * @param fileContent
     * @param filename
     * @param fileDirectory
     */
    public static void writeToExistingFile(String fileContent, String filename, String fileDirectory)
    {
        try{
            File file = new File(fileDirectory + filename);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file,true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(fileContent);
            bw.close();    
          }catch(IOException ioe){
             System.out.println("Exception occurred:");
             ioe.printStackTrace();
           }
    }

    /**
     *
     * @param filepath
     * @param filename
     * @return
     */
    public static boolean createNewFile(String filepath, String filename) {
        boolean createStatus = false;
        try {
            File file = new File(filepath + filename);
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return createStatus;
    }

    private static boolean dotInFileName(String fileName) {
        if (fileName == null || fileName.isEmpty())
            return false;
        return fileName.contains(".");
    }

    /**
     *
     * @param filepath
     * @param folderName
     * @return
     */
    public static boolean createNewFolder(String filepath, String folderName) {
        StringBuilder sb = new StringBuilder();
        if (filepath != null)
            sb.append(filepath).append("/");
        if (folderName != null)
            sb.append(folderName);
        File f=new File(sb.toString());
        f.mkdir();
        return true;
    }

    /**
     * @param filePath
     * @return
     * @throws IOException
     */
    public static boolean createNewFolderRecursively(String filePath) {
        boolean createStatus = false;
        String[] folders = filePath.split("/");
        if (folders.length >= 2) {
            StringBuilder sb = new StringBuilder("./");
            for (String eachFolder : folders) {
                sb.append(eachFolder);
                createStatus = createNewFolder(sb.toString(), null);
                if (!createStatus)
                    break;
                else
                    sb.append("/");
            }
        }
        return createStatus;
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
    public static ArrayList<Map<String, Object>> readPsvFileForQueryOps(String filePath) throws FileNotFoundException {
        ArrayList result = new ArrayList();
        ArrayList columns = new ArrayList();
        File fileObject = new File(filePath);
        Scanner sc = new Scanner(fileObject);
        int count = 0;
        while (sc.hasNext()) {
            if (count == 0) {
                columns = getArrayForPipeString(sc.nextLine());
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
                    ArrayList rowData = getArrayForPipeString(sc.nextLine());
                    if (rowData != null) {
                        Map dataDict = new HashMap<String, Object>();
                        for (int i = 0; i < columns.size(); i++) {
                            dataDict.put(columns.get(i), rowData.get(i));
                        }
                        result.add(dataDict);
                    }
                }
            }
        }
        sc.close();
        return result;
    }


    public List<List<Object>> readDataFromPSV(String filePath) {
        List<List<Object>> rows = new ArrayList<>();
        List<Object> columnValues;

        String path;
        if (filePath.contains(".psv")) {
            path = filePath;
        } else {
            path = filePath + ".psv";
        }
        File file = new File(path);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                columnValues = new ArrayList<>(Arrays.asList(line.split(MiscConstants.PIPE, -1)));
                rows.add(columnValues);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return rows;
    }

    public static void writeDataToPSV(List<List<Object>> rows, String filePath) {
        FileWriter psvWriter = null;
        try {
            psvWriter = new FileWriter(filePath+".psv");
            for (List<Object> rowData : rows) {
                psvWriter.append(rowData.toString().replace("[", "").replace("]", "").replaceAll(",", "|"));
                psvWriter.append("\n");
            }
            psvWriter.flush();
            psvWriter.close();
        } catch (Exception e) {
            e.getCause();
        }
    }

    public void writeStringToPSV(String row, String filePath) {
        FileWriter psvWriter = null;
        try {
            psvWriter = new FileWriter(filePath, true);
            psvWriter.append(row);
            psvWriter.append("\n");
            psvWriter.flush();
            psvWriter.close();
        } catch (Exception e) {
            e.getCause();
        }
    }
}
