package com.sapashev;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Sorts file by lines length.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class SortLines {
    final Logger LOG = LoggerFactory.getLogger(SortLines.class);

    public static void main (String[] args) throws Exception {
        new SortLines().start(args);
    }

    /**
     * Invokes methods:
     * 1) to read from source file (readFromFileTo())
     * 2) to create packed long value to store pair position:line length (readFromFileTo())
     * 3) to sort internal buffer with pairs of position:line length (quickSort())
     * 4) to save sorted pairs to the temporary file (saveToTempFile())
     * 5) to sort and combine all temp files to the final one (reduce())
     * 6) to copy lines from source file to destination file (copyFromSourceToDestination())
     * @param args :
     * 1) name of source file
     * 2) name of destination file
     * 3) charset of source file
     * 4) buffer size - size of internal buffer to temporary store of pairs (measured in county of long)
     * 5) number of less significant bits of long value to store length of line (measured in chars)
     * 6) number of more significant bits to store position of line start (measured in bytes)     *
     * @throws Exception
     */
    public void start(String[] args) throws Exception{
        FileInputStream fis = new FileInputStream(args[0]);
        InputStreamReader isr = new InputStreamReader(fis, args[2]);
        boolean isEOF = false;
        int charSize = Charset.forName(args[2]).encode("s").limit();
        int bufferSize = Integer.parseInt(args[3]);
        long[] metas = new long[bufferSize];
        Reader reader = new Reader();
        Sorter sorter = new Sorter();
        Packer packer = new Packer(Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        Combiner combiner = new Combiner();
        List<File> temps = new ArrayList<>();
        Counter counter = new Counter();
        Position position = new Position();

        try(BufferedReader br = new BufferedReader(isr)){
            while (!isEOF){
                LOG.info("File read started at {}", LocalTime.now());
                isEOF = reader.readFromFileTo(metas, charSize, packer, br, counter, position);
                LOG.info("File read finished at {}", LocalTime.now());
                LOG.info("Array trunking started at {}", LocalTime.now());
                metas = Arrays.copyOfRange(metas,0,counter.counter);
                LOG.info("Array trunking finished at {}", LocalTime.now());
                LOG.info("Array sorting started at {}", LocalTime.now());
                sorter.quickSort(metas, 0, metas.length-1, packer);
                LOG.info("Array sorting finished at {}", LocalTime.now());
                LOG.info("Saving to temp file started at {}", LocalTime.now());
                temps.add(combiner.saveToTempFile(metas));
                LOG.info("Saving to temp file finished at {}", LocalTime.now());
                metas = Arrays.copyOf(metas, bufferSize);
            }
        }
        temps.forEach(File::deleteOnExit);
        LOG.info("Reducing started at {}", LocalTime.now());
        File result = reduce(temps, packer);
        LOG.info("Reducing finished at {}", LocalTime.now());
        LOG.info("Copying to desti started at {}", LocalTime.now());
        copyFromSourceToDestination(args, result, packer);
        LOG.info("Copying to desti finished at {}", LocalTime.now());
    }

    /**
     * Copies lines from source file to destination one.
     * To the end of each line appends bytes of "line.separator" property
     * @param args - args of program
     * @param result - temporary file which contains sorted long values (defines pair of position:line length)
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value
     * @throws IOException
     */
    private void copyFromSourceToDestination (String[] args, File result, Packer p) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(args[0], "r");
        final FileOutputStream fos = new FileOutputStream(args[1]);
        final BufferedOutputStream bos = new BufferedOutputStream(fos);
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(result)))) {
            final int LONG_SIZE = 8;
            long l;
            int numBytes;
            long position;
            int charSize = Charset.forName(args[2]).encode("s").limit();
            byte[] separator = Charset.forName(args[2]).encode(System.getProperty("line.separator")).array();

            long limit = result.length() / LONG_SIZE;
            for(int i = 0; i < limit; i++){
                l = dis.readLong();
                numBytes = p.getLength(l) * charSize;
                position = p.getPosition(l);
                raf.seek(position);
                byte[] buffer = new byte[numBytes];
                if (raf.read(buffer) == -1) {
                    throw new EOFException(String.format("Unexpected end of file at position %s", position));
                }
                bos.write(buffer);
                bos.write(separator);
            }
        }
    }

    /**
     * Reduces (combines) temp files to final one. Which contains all pairs (position:line length) in sorted manner.
     * After each iteration of merging content of files, file from "temps" list is about to be deleted.
     * @param temps - list of temp files to combine.
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value.
     * @return - final combined file, which contains sorted long values from all other temp files.
     * @throws IOException
     */
    private File reduce(List<File> temps, Packer p) throws IOException {
        File f = File.createTempFile("ars_reduce_", null);
        f.deleteOnExit();
        while(temps.size() >= 1){
            f = new Combiner().mergeToOne(f, temps.get(0), p);
            temps.remove(0);
        }
        return f;
    }

    /**
     * Inner class describes counter - number of characters read from source file on each iteration.
     */
    public class Counter{
        int counter;
    }

    /**
     * Inner class describes position of line which has been read from source file.
     */
    public class Position {
        long position;
    }
}
