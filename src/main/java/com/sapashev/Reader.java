package com.sapashev;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reads lines from source file packs line start position (in bytes) and line length (in chars)
 * to the one compound long value;
 * After that each long value stores to array of long named metas.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class Reader {
    private final int SEPARATOR = ByteBuffer.wrap(System.getProperty("line.separator").getBytes()).limit();
    //TODO define getBytes encoding as entry of argument list

    /**
     * Fills metas array with line position and length data.
     * @param charSize - size in bytes of each character in that particular charset.
     * @param br - BufferedReader reference.
     * @return - true if end-of-file has been reached, or false if not.
     * @throws IOException
     */
    public boolean readFromFileTo (List<Line> lines, int charSize, int bufferSize,
                                   BufferedReader br, SortLines.Counter c, SortLines.Position p) throws IOException {
        String line;
        int counter = 0;
        boolean isEOF = false;
        c.counter = 0;

        while (counter < bufferSize){
            if(((line = br.readLine()) != null)){
                lines.add(new Line(p.position, line.length()));
                counter++;
                //TODO choose one of them
                //p.position = p.position + ((line.length() + SEPARATOR) * charSize);
                p.position = p.position + ((line.length() * charSize) + SEPARATOR);
            } else {
                isEOF = true;
                break;
            }
        }
        c.counter = counter;
        return isEOF;
    }
}
