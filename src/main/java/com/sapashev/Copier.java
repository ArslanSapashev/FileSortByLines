package com.sapashev;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Copies lines from source to destination.
 * @author Arslan Sapashev
 * @since 27.02.2017
 * @version 1.0
 */
public class Copier {
    /**
     * Copies lines from source file to destination using direct byte buffers of NIO.
     * @param args - argument list
     * @param source - source file
     * @param reference - file with long values pointing to the beginning and length of each line.
     * @param p - packer to unpack position/length from the composed long value.
     * @return - result file.
     * @throws IOException
     */
    public Path directCopy (String[] args, File source, File reference, Packer p) throws IOException {
        Path result = Files.createFile(Paths.get(args[1])).toAbsolutePath();
        try(RandomAccessFile raf = new RandomAccessFile(source, "rw");
            FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            FileChannel fcResult = FileChannel.open(result, StandardOpenOption.READ, StandardOpenOption.WRITE)){

            MappedByteBuffer src = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_WRITE, 0, fcRef.size());
            MappedByteBuffer res = fcResult.map(FileChannel.MapMode.READ_WRITE, 0, raf.length() + 2);

            byte[] bb = new byte[10000];
            int position = 0;
            int length = 0;

            while(ref.hasRemaining()){
                long value = ref.getLong();
                position = (int) p.getPosition(value);
                length = p.getLength(value);
                src.position(position);
                src.get(bb, 0, length);
                res.put(bb, 0, length);
                res.put("\r\n".getBytes("UTF-8"));
            }
        }
        return result;
    }
}
