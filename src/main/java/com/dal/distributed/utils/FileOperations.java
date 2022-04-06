package com.dal.distributed.utils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;


// Class for file operations
public class FileOperations {
    static PrintWriter printWriter;
    
    // Read files from the directory
    public static File[] readFiles(String dir) {
        File file = new File(dir);
        return file.listFiles();
    }

    //Read file content from specific file, returns String content
    public static String readFileContent(File file) throws IOException
    {
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
    public static void writeToNewFile(String fileContent, String filename, String fileDirectory) throws FileNotFoundException
    {

        try{
        printWriter = new PrintWriter(fileDirectory+filename);
        printWriter.write(fileContent);
        printWriter.flush();
        printWriter.close();
        }
        catch(FileNotFoundException f)
        {
            System.out.println("FileNotFound: Change path of the directory in IProperties.class");
        }
    }

    public static void writeToExistingFile(String fileContent, String filename, String fileDirectory)
    {
        try{
            
            File file =new File(fileDirectory+filename);
            if(!file.exists()){
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


    public static boolean createNewFile(String filepath, String filename) throws IOException
    {
        boolean createStatus = false;
        try{
            File f=new File(filepath+"/"+filename+".psv");
            f.createNewFile();
            createStatus = true;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return createStatus;
    }

    public static boolean createNewFolder(String filepath, String folderName) throws IOException
    {
        boolean createStatus = false;
        try{
            File f=new File(filepath+"/"+folderName);
            f.mkdir();
            return createStatus;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return createStatus;
    }

    public static boolean createNewFolderRecursively(String filePath) throws IOException {
        boolean createStatus = false;
        String[] folders = filePath.split("/");
        if(folders.length >= 2){
            StringBuilder sb = new StringBuilder();
            for(String eachFolder:folders){
                sb.append(eachFolder);
                createStatus = createNewFolder(sb.toString(), null);
                if(!createStatus)
                    break;
                else
                    sb.append("/");
            }
        }
        return createStatus;
    }
}
