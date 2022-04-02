package com.dal.distributed.utils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;


// Class for file operations
public class FileOperations {
    PrintWriter printWriter;
    
    // Read files from the directory
    public File[] readFiles(String dir) {
        File file = new File(dir);
        return file.listFiles();
    }

    //Read file content from specific file, returns String content
    public String readFileContent(File file) throws IOException
    {
        StringBuilder jString = new StringBuilder();
        Scanner scanner = new Scanner(file, StandardCharsets.ISO_8859_1);
            while (scanner.hasNextLine()) {
                jString.append(scanner.nextLine());
                jString.append(System.lineSeparator());
            }
            scanner.close();
        return jString.toString();

    }

    //Write file to the given directory
    public void writeToNewFile(String fileContent, String filename, String fileDirectory) throws FileNotFoundException
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

    public void writeToExistingFile(String fileContent, String filename, String fileDirectory)
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


    public boolean createNewFile(String filepath, String filename) throws IOException
    {
        File f=new File(filepath+"/"+filename+".psv");
        f.createNewFile();
        return true;
    }

    public boolean createNewFolder(String filepath, String folderName) throws IOException
    {
        File f=new File(filepath+"/"+folderName);
        f.mkdir();
        return true;
    }
}
