package com.dal.distributed.ERD_Model;

import java.util.Scanner;

public class ScreenReader
{
    private Scanner scanner = new Scanner(System.in);

    public String readString() {
        return scanner.nextLine();
    }

    public int readInt() {
        int number = scanner.nextInt();
        scanner.nextLine();
        return number;
    }
}
