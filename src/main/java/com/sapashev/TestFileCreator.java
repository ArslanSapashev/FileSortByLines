package com.sapashev;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Creates test file.
 * Lines filled up with 'a' character.
 * Length of each line determines dynamically by Random object, but not more than maxLineLength argument.
 * First argument - new file name
 * Second argument - how much lines should have test file.
 * Third argument - maximum line length.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class TestFileCreator {
    public static void main (String[] args) throws IOException {
        Random r = new Random();
        FileWriter fr = new FileWriter(args[0]);
        int howMuchLines = Integer.parseInt(args[1]);
        int maxLineLength = Integer.parseInt(args[2]);
        StringBuilder sb = new StringBuilder();
        String sep = System.getProperty("line.separator");

        for(int i = 0; i < howMuchLines; i++){
            for(int y = 0; y < (r.nextInt(maxLineLength)+25); y++){
                sb.append('a');
            }
            fr.write(String.format("%s%s", sb.toString(),sep));
            sb.delete(0, sb.length());
        }
        fr.close();
    }
}