package com.dal.distributed.utils;

import com.dal.distributed.constant.MiscConstants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;


// Class for file operations
public class FileOperations {
    static PrintWriter printWriter;

    // Read files from the directory

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
    public static void writeToExistingFile(String fileContent, String filename, String fileDirectory) {
        try {

            File file = new File(fileDirectory + filename);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(fileContent);
            bw.close();
        } catch (IOException ioe) {
            System.out.println("Exception occurred:");
            ioe.printStackTrace();
        }
    }

    /**
     * @param filepath
     * @param filename
     * @return
     * @throws IOException
     */
    public static boolean createNewFile(String filepath, String filename) throws IOException {
        boolean createStatus = false;
        try {
            File f = new File(filepath + "/" + filename + ".psv");
            f.createNewFile();
            createStatus = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return createStatus;
    }

    /**
     * @param filepath
     * @param folderName
     * @return
     * @throws IOException
     */
    public static boolean createNewFolder(String filepath, String folderName) throws IOException {
        boolean createStatus = false;
        StringBuilder sb = new StringBuilder();
        if (filepath != null)
            sb.append(filepath).append("/");
        if (folderName != null)
            sb.append(folderName);
        try {
            File f = new File(sb.toString());
            f.mkdir();
            createStatus = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return createStatus;
    }

    /**
     * @param filePath
     * @return
     * @throws IOException
     */
    public static boolean createNewFolderRecursively(String filePath) throws IOException {
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
    public static String[] getArrayForPipeString(String text) {
        if (text != null)
            return text.split(MiscConstants.PIPE);
        else
            return null;
    }

    /**
     * @param filePath
     * @return ArrayList - first element is {"columns" : []}, second onwards - Map<>
     * @throws Exception
     */
    public static ArrayList<Map<String, Object>> readPsvFileForQueryOps(String filePath) throws Exception {
        ArrayList result = new ArrayList();
        String[] columns = new String[0];
        File fileObject = new File(filePath);
        Scanner sc = new Scanner(fileObject);
        int count = 0;
        while (sc.hasNext()) {
            if (count == 0) {
                columns = getArrayForPipeString(sc.nextLine());
                if (columns == null)
                    break;
                else {
                    String[] finalColumns = columns;
                    Map dataDict = new HashMap() {{
                        put("columns", Arrays.asList(finalColumns));
                    }};
                    result.add(dataDict);
                    count++;
                }
            } else {
                if (columns.length == 0)
                    break;
                else {
                    String[] rowData = getArrayForPipeString(sc.nextLine());
                    if (rowData != null) {
                        Map dataDict = new HashMap<String, Object>();
                        for (int i = 0; i < columns.length; i++) {
                            dataDict.put(columns[i], rowData[i]);
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
        List<Object> columnValues = new ArrayList<>();
        File file = new File(filePath+".psv");
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

    public void writeDataToPSV(List<List<Object>> rows, String filePath) {
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
}
