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
    public File mergeToOne (File first, File second, Packer p) throws IOException{
        File f = File.createTempFile("arsMERGE_", null);
        f.deleteOnExit();
        try(DataOutputStream result = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
            PushbackInputStream source1 = new PushbackInputStream((new BufferedInputStream(new FileInputStream(first))), 8192);
            PushbackInputStream source2 = new PushbackInputStream((new BufferedInputStream(new FileInputStream(second))), 8192)
        ) {
            boolean isSourceEmpty = false;
            ByteBuffer bb = ByteBuffer.allocate(8);

            while (!isSourceEmpty){
                long l1 = getLongFrom(source1);
                long l2 = getLongFrom(source2);

                if(l1 == -1){
                    isSourceEmpty = true;
                    if(l2 != -1){
                        result.writeLong(l2);
                    }
                    copyRestFrom(source2, result);
                } else if(l2 == -1){
                    isSourceEmpty = true;
                    if(l1 != -1){
                        result.writeLong(l1);
                    }
                    copyRestFrom(source1, result);
                } else {
                    writeTheLess(p, result, source1, source2, bb, l1, l2);
                }
            }
        } return f;
    }

    /**
     * Writes to the compound file, the less value among two values read form both of files.
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value.
     * @param result - reference to the DataOutputStream to which writes sorted long values;
     * @param source1 - reference to the one of the files;
     * @param source2 - reference to the second one;
     * @param bb - byte buffer to store long value to be pushed back if it was greater than another one.
     * @param l1 - long value to be compared and stored if it less than another one, read from first file.
     * @param l2 - long value to be compared and stored if it less than another one, read from second file.
     * @throws IOException
     */
    private void writeTheLess (Packer p, DataOutputStream result, PushbackInputStream source1, PushbackInputStream source2, ByteBuffer bb, long l1, long l2) throws IOException {
        if(p.getLength(l1) <= p.getLength(l2)){
            result.writeLong(l1);
            source2.unread(bb.putLong(l2).array());
            bb.clear();
        } else {
            result.writeLong(l2);
            source1.unread(bb.putLong(l1).array());
            bb.clear();
        }
    }

    /**
     * In case of content of the one of files eliminates before content of another one,
     * it copies the rest of the file to the final compound file.
     * @param source - file which contains the long values to be copied to the final file.
     * @param result - result (compound)file.
     * @throws IOException
     */
    private void copyRestFrom(PushbackInputStream source, DataOutputStream result) throws IOException{
        while (source.available() >= 8){
            result.writeLong(getLongFrom(source));
        }
    }

    /**
     * Reads next long value from the source stream, if number of files read from the stream are less then 8
     * it returns -1, which mars the end of stream (file).
     * @param source - source stream (file).
     * @return - next long value which has been read from source stream;
     * @throws IOException
     */
    private long getLongFrom (PushbackInputStream source) throws IOException {
        long l;
        byte[] array = new byte[8];
        if(source.read(array, 0, 8) >= 8){
            l = ByteBuffer.wrap(array).getLong();
        } else {
            l = -1;
        }
        return l;
    }

    /**
     * Another one version of mergeToOne method. Works a little bit slowly than mergeToOne method.
     * @param first - first source file.
     * @param second - second source file.
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value.
     * @return - compound file.
     * @throws IOException
     */
    public File mergeV2(File first, File second, Packer p) throws IOException {
        File f = File.createTempFile("arsMERGE_V2_", null);
        f.deleteOnExit();
        try(DataInputStream dis1 = new DataInputStream(new BufferedInputStream(new FileInputStream(first)));
            DataInputStream dis2 = new DataInputStream(new BufferedInputStream(new FileInputStream(second)));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))){

            Getter getter = new Getter(dis1, dis2, p);
            long l;
            while ((l = getter.getNextLessLong()) != -1){
                dos.writeLong(l);
            }
        }
        return f;
    }

    /**
     * Inner class that encapsulates process of choosing the less value from two sources.
     */
    private class Getter{
        private final DataInputStream dis1;
        private final DataInputStream dis2;
        private final Packer p;
        private long l1 = -1L;
        private long l2 = -1L;
        private long result = -1L;
        private DIS chosen = DIS.NONE;

        public Getter(DataInputStream dis1, DataInputStream dis2, Packer p){
            this.dis1 = dis1;
            this.dis2 = dis2;
            this.p = p;
        }

        /**
         * Compares two long values based on the packed length of line value, and returns the less one.
         * It reads long values from sources in value-by-value manner, and only if the previous value read
         * from the particular source stream was returned as the less one
         * (in fact it means that it was written to the compound file).
         * @return - the less long value among two value;
         * @throws IOException
         */
        public long getNextLessLong () throws IOException{
            result = -1L;

            if(dis1.available() >= 8 && (chosen == DIS.FIRST || chosen == DIS.NONE.NONE)) {
                l1 = dis1.readLong();
            }
            if(dis2.available() >= 8 && (chosen == DIS.SECOND || chosen == DIS.NONE)) {
                l2 = dis2.readLong();
            }
            if(l1 != -1L && (l2 == -1L || p.getLength(l1) <= p.getLength(l2))){
                result = l1;
                chosen = DIS.FIRST;
                l1 = -1L;
            } else if (l2 != -1L){
                result = l2;
                chosen = DIS.SECOND;
                l2 = -1L;
            }
            return result;
        }
    }
    private enum DIS {
        FIRST, SECOND, NONE;
    }

}
