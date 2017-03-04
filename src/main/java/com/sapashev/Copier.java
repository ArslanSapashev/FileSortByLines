package com.sapashev;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Copies lines from source to destination.
 * @author Arslan Sapashev
 * @since 27.02.2017
 * @version 1.0
 */
public class Copier {
    int boundary = Integer.MAX_VALUE;       //buffer size
    /**
     * Copies lines from source file to destination using direct byte buffers of NIO.
     * @param args - argument list
     * @param reference - file with long values pointing to the beginning and length of each line.
     * @param p - packer to unpack position/length from the composed long value.
     * @return - result file.
     * @throws IOException
     */
    public Path directCopy (String[] args, File reference, Packer p) throws IOException {
        Path result = Files.createFile(Paths.get(args[1])).toAbsolutePath();
        try(RandomAccessFile raf = new RandomAccessFile(args[0], "r");
            FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ);
            FileChannel fcResult = FileChannel.open(result, StandardOpenOption.READ, StandardOpenOption.WRITE)){

            MappedByteBuffer src = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_ONLY, 0, fcRef.size());
            MappedByteBuffer res = fcResult.map(FileChannel.MapMode.READ_WRITE, 0, raf.length() + 2);

            byte[] bb = new byte[longestLineLength(reference, p)];
            int position = 0;
            int length = 0;

            while(ref.hasRemaining()){
                long value = ref.getLong();
                position = (int) p.getPosition(value);
                length = p.getLength(value);
                src.position(position);
                src.get(bb, 0, length);
                res.put(bb, 0, length);
                res.put("\r\n".getBytes(args[2]));
            }
        }
        return result;
    }

    /**
     * Creates list of MappedByteBuffers mapped to the appropriate regions of source file.
     * @param source - source file.
     * @return list of MappedByteBuffers.
     * @throws IOException
     */
    private List<MappedByteBuffer> sourceBuffers (File source) throws IOException {
        List<MappedByteBuffer> list = new ArrayList<>();
        MappedByteBuffer buf;
        long numBuffers =   (source.length() % boundary) > 0 ?
                            (source.length() / boundary) + 1 :
                            (source.length() / boundary);

        try(RandomAccessFile raf = new RandomAccessFile(source, "r")){
            for(int i = 0; i < numBuffers; i++){
                long position = (long)i * (long)boundary;
                int length;
                long chunk = (i + 1) * (long)boundary;
                if(chunk > raf.length()){
                    length = (int)(raf.length() % boundary);
                } else {
                    length = boundary;
                }
                buf = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, length);
                list.add(buf);
            }
        }
        return list;
    }

    /**
     * Creates list of MappedByteBuffers mapped to the consecutive regions of result file.
     * @param args - command-line argument list.
     * @param sources - list of MappedByteBuffers mapped to the source file.
     * @return - list of MappedByteBuffers.
     * @throws IOException
     */
    private List<MappedByteBuffer> resultBuffers (String[] args,List<MappedByteBuffer> sources) throws IOException {
        List<MappedByteBuffer> results = new ArrayList<>(sources.size());
        Path file = Files.createFile(Paths.get(args[1])).toAbsolutePath();
        try(FileChannel fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)){
            MappedByteBuffer r;
            for(int i = 0; i < sources.size(); i++){
                long position = i * (long)boundary;
                if(sources.get(i).capacity() < boundary){
                    r = fc.map(FileChannel.MapMode.READ_WRITE, position, sources.get(i).capacity() + 2);
                } else {
                    r = fc.map(FileChannel.MapMode.READ_WRITE, position, sources.get(i).capacity());
                }
                results.add(r);
            }
        }
        return results;
    }

    /**
     * Retrieves lines from source file and saves it to the final one.
     * Source and result files are divided to the appropriate number of MappedByteBuffers.
     * On each iteration of while cycle:
     * 1) long value are read from reference file, position and length of each line determined.
     * 2) according to the offset of line in source file appropriate MappedByteBuffer will be chosen.
     * 2.1) if line starts at one buffer and finishes at the next one, then line will be read in two steps
     * 2.1.1) first chunk will be read from current source buffer
     * 2.1.2) second chunk will be read from the next source buffer
     * 3) each line will be saved to the current result MappedByteBuffer.
     * @param args - command-line arguments.
     * @param reference - file which contains sorted long values that are reference to lines in source file.
     * @param p - packer object.
     * @throws IOException
     */
    public void multiBufferDirectCopy (String[] args, File reference, Packer p) throws IOException {
        List<MappedByteBuffer> sources = sourceBuffers(new File(args[0]));
        List<MappedByteBuffer> results = resultBuffers(args, sources);
        byte[] separator = System.getProperty("line.separator").getBytes(args[2]);
        try(FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ)){

            byte[] buf = new byte[longestLineLength(reference, p)];
            long position;
            int length;

            MappedByteBuffer src;
            MappedByteBuffer res = results.get(0);
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_ONLY, 0, fcRef.size());

            while(ref.hasRemaining()){
                long value = ref.getLong();
                position = p.getPosition(value);
                int relativePosition = (int)(position % (long)boundary);                    //calculates position of line relative to the start of current buffer.
                length = p.getLength(value);
                int ordinal = (int)(position / boundary);                                   //calculates which buffer from list to choose.
                src = sources.get(ordinal);                                                 //gets appropriate source MappedByteBuffer.

                if(((long)relativePosition + (long)length) > src.limit()){                  //if line lies on boundary of two consecutive buffers.
                    src.position(relativePosition);
                    int firstChunk = src.limit() - relativePosition;                        //calculates length of first chunk which fits to the limit of current buffer.
                    int secondChunk = length - firstChunk;
                    //first chunk read & writing
                    src.get(buf, 0, firstChunk);
                    res = putToResultBuffer(results, res, buf, firstChunk);
                    //second chunk read & writing
                    src = sources.get(ordinal + 1);
                    src.position(0);
                    src.get(buf, 0, secondChunk);
                    res = putToResultBuffer(results, res, buf, secondChunk);
                    res = putToResultBuffer(results, res, separator, separator.length);     //saves line.separator at the end of line.
                } else {
                    src.position(relativePosition);
                    src.get(buf, 0, length);
                    res = putToResultBuffer(results, res, buf, length);
                    res = putToResultBuffer(results, res, separator, separator.length);
                }
            }
            res.force();
        }
        return;
    }

    /**
     * Saves bytes from byte buffer to the appropriate MappedByteBuffer mapped to the result file.
     * In case of current buffer has no enough room to save whole line, the line will be divided into two chunks.
     * First chunk will be written to the current buffer, and the second chunk will be written to the next buffer.
     * In that case new buffer will be returned as new current.
     * @param results - list of MappedByteBuffers mapped to the some regions of the result file.
     * @param current - current MappedByteBuffer to which lines will be written in.
     * @param buffer - byte buffer which contains lines to be written in.
     * @param length - length of line.
     * @return - current MappedByteBuffer.
     */
    private MappedByteBuffer putToResultBuffer(List<MappedByteBuffer> results, MappedByteBuffer current, byte[] buffer, int length){
        MappedByteBuffer internal = current;
        int index = results.indexOf(current);
        if(internal.position() + (long)length > internal.limit()){              //if the line longer than the current buffer length.
            int firstChunk = internal.limit() - internal.position();            //calculates first chunk length
            internal.put(buffer, 0, firstChunk);                                // saves first chun to the current MappedByteBuffer
            internal = results.get(index + 1);                                  // assigns to the internal reference to the next MappedByteBuffer
            internal.position(0);                                               // sets position of new buffer to the 0;
            internal.put(buffer, firstChunk, length - firstChunk);              // saves second chunk to the new buffer.
        } else {
            internal.put(buffer, 0, length);                                    //if line fits to the current buffer
        }
        return internal;
    }

    private int longestLineLength(File reference, Packer p) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(reference, "r")){
            raf.seek(raf.length() - 8);
            return p.getLength(raf.readLong());
        }
    }
}
