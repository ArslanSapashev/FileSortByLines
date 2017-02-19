package com.sapashev;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Combines all sorted arrays of meta information about lines in one final file.
 * Every array will be written to the temp file. And when all meta information from source file will be read,
 * all partial arrays stored in temp files will be merged to the one sorted file.
 * @author Arslan Sapashev
 * @since 01.01.2017
 * @version 1.0
 */
public class Combiner {

    /**
     * Creates temporary file and then saves long values from metas array to that file.
     * @param metas - array of long value to be stored to temp file.
     * @return - temp file which contains all values from array.
     * @throws IOException
     */
    public File saveToTempFile(long[] metas) throws IOException{
        File f = File.createTempFile("arsRAW_", null);
        f.deleteOnExit();
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f));
        try(DataOutputStream dos = new DataOutputStream(bos)){
            for(long l : metas){
                dos.writeLong(l);
            }
        }
        return f;
    }

    /**
     * Merges content of two files to the one file. Values of both temp files will be stored to the final file
     * in sorted ascending manner.
     * Compares long values based on the length of line, stored in less significant bits of each long value;
     * @param first - file which content to be sorted and stored to the final compound file.
     * @param second - file which content to be sorted and stored to the final compound file.
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value.
     * @return - final file which contains merged values of both temp files.
     * @throws IOException
     */
    public File mergeToOne (File first, File second, Packer p) throws IOException {
        File f = File.createTempFile("arsMERGE_", null);
        f.deleteOnExit();
        try(DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
            DataInputStream dis_1 = new DataInputStream(new BufferedInputStream(new FileInputStream(first)));
            DataInputStream dis_2 = new DataInputStream(new BufferedInputStream(new FileInputStream(second)))){

            final long EMPTY = Long.MIN_VALUE;
            long l1 = EMPTY;
            long l2 = EMPTY;
            long dis_1_entries = dis_1.available() / 8;
            long dis_2_entries = dis_2.available() / 8;

            do{
                if(l1 == EMPTY && dis_1_entries > 0){
                    l1 = dis_1.readLong();
                    --dis_1_entries;
                }
                if(l2 == EMPTY && dis_2_entries > 0){
                    l2 = dis_2.readLong();
                    --dis_2_entries;
                }
                if(p.getLength(l1) < p.getLength(l2)){
                    dos.writeLong(l1);
                    l1 = EMPTY;
                } else {
                    dos.writeLong(l2);
                    l2 = EMPTY;
                }
            } while ((dis_1_entries > 0 || l1 != EMPTY) && (dis_2_entries > 0 || l2 != EMPTY));

            if (dis_1_entries <= 0 && dis_2_entries >= 0){
                if (l2 != EMPTY) dos.writeLong(l2);
                byte[] buffer = new byte[dis_2.available()];
                dis_2.read(buffer);
                dos.write(buffer);
            }
            if(dis_1_entries >= 0 && dis_2_entries <= 0){
                if (l1 != EMPTY) dos.writeLong(l1);
                byte[] buffer = new byte[dis_1.available()];
                dis_1.read(buffer);
                dos.write(buffer);
            }
        }
        return f;
    }



}
