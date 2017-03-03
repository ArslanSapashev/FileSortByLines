package com.sapashev;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Sony on 01.03.2017.
 */
public class SortChecker {
    public static void main (String[] args) throws IOException {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])))){
            int prevLength = 0;
            String line;
            String prevLine = "";
            long readBytes = 0L;
            int readLines = 0;
            int violatios = 0;
            while ((line = br.readLine()) != null){
                if(line.length() < prevLength){
                    System.out.println("Violation");
                    System.out.println("readBytes = " + readBytes);
                    System.out.println("readLines = " + readLines);
                    System.out.println(String.format("line length =%s prevLength=%s", line.length(), prevLength));
                    violatios++;
                }
                readLines++;
                readBytes += line.length() + 2;
                prevLength = line.length();
                prevLine = line;
            }
            System.out.println("Number of violations " + violatios);
        }
    }
}
