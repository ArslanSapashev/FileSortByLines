package com.sapashev;

import java.io.*;
import java.util.Arrays;

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
            copyTheRest(dos, dis_1, dis_2, EMPTY, l1, l2, dis_1_entries, dis_2_entries);
        }
        return f;
    }

    /**
     * Selects InputStream from which to copy the rest bytes to the OutputStream.
     * Will be invoked when all long values from one file retrieved and another one still has long values.
     * @param dos - target file stream.
     * @param dis_1 - first file.
     * @param dis_2 - second file.
     * @param EMPTY - mark that value is already read.
     * @param l1 - value from first file.
     * @param l2 - value from second file.
     * @param dis_1_entries - number of values are still unread in the first file.
     * @param dis_2_entries - number of values are still unread in the second file.
     * @throws IOException
     */
    private void copyTheRest (DataOutputStream dos, DataInputStream dis_1, DataInputStream dis_2, long EMPTY, long l1, long l2, long dis_1_entries, long dis_2_entries) throws IOException {
        if (dis_1_entries <= 0 && dis_2_entries >= 0){
            copyAllBytes(dos, dis_2, EMPTY, l2);
        }
        if(dis_1_entries >= 0 && dis_2_entries <= 0){
            copyAllBytes(dos, dis_1, EMPTY, l1);
        }
    }

    /**
     * Copies all rest bytes from source file to the target file.
     * @param dos - target file.
     * @param dis - source file.
     * @param EMPTY - mark that value is already read.
     * @param l - value which is already read from source file, but not yet written to the target file.
     * @throws IOException
     */
    private void copyAllBytes (DataOutputStream dos, DataInputStream dis, long EMPTY, long l) throws IOException {
        if (l != EMPTY) dos.writeLong(l);
        byte[] buffer = new byte[dis.available()];
        int readBytes = dis.read(buffer);
        dos.write(Arrays.copyOfRange(buffer,0, readBytes));
    }
}
