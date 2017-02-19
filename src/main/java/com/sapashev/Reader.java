package com.sapashev;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private final Logger LOG = LoggerFactory.getLogger(Reader.class);
    private final int SEPARATOR = ByteBuffer.wrap(System.getProperty("line.separator").getBytes()).limit();

    /**
     * Fills metas array with line position and length data.
     * @param metas - array to store meta information (start position, length) about each line of source file.
     * @param charSize - size in bytes of each character in that particular charset.
     * @param br - BufferedReader reference.
     * @return - true if end-of-file has been reached, or false if not.
     * @throws IOException
     */
    public boolean readFromFileTo (long[] metas, int charSize, Packer packer,
                                   BufferedReader br, SortLines.Counter c, SortLines.Position p) throws IOException {
        String line;
        int counter = 0;
        Arrays.fill(metas, 0L);
        boolean isEOF = false;
        c.counter = 0;

        while (counter < metas.length){
            if(((line = br.readLine()) != null)){
                metas[counter] = packer.packToLong(line.length(), p.position);
                counter++;
                p.position = p.position + ((line.length() + SEPARATOR) * charSize);
            } else {
                isEOF = true;
                break;
            }
        }
        c.counter = counter;
        return isEOF;
    }

    /**
     * Fills metas array with line position and length data.
     * @param charSize - size in bytes of each character in that particular charset.
     * @param br - BufferedReader reference.
     * @return - true if end-of-file has been reached, or false if not.
     * @throws IOException
     */
    public boolean read2 (List<Line> lines, int charSize, int bufferSize,
                          BufferedReader br, SortLines.Counter c, SortLines.Position p) throws IOException {
        String line;
        int counter = 0;
        boolean isEOF = false;
        c.counter = 0;

        while (counter < bufferSize){
            if(((line = br.readLine()) != null)){
                lines.add(new Line(p.position, line.length()));
                counter++;
                p.position = p.position + ((line.length() + SEPARATOR) * charSize);
            } else {
                isEOF = true;
                break;
            }
        }
        c.counter = counter;
        return isEOF;
    }
}
