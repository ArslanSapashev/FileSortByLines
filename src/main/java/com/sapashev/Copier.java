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
        try(RandomAccessFile raf = new RandomAccessFile(source, "r");
            FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ);
            FileChannel fcResult = FileChannel.open(result, StandardOpenOption.READ, StandardOpenOption.WRITE)){

            MappedByteBuffer src = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_ONLY, 0, fcRef.size());
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

    private List<MappedByteBuffer> sourceBuffers (File source) throws IOException {
        List<MappedByteBuffer> list = new ArrayList<>();
        MappedByteBuffer buf;
        long numBuffers =   (source.length() % Integer.MAX_VALUE) > 0 ?
                            (source.length() / Integer.MAX_VALUE) + 1 :
                            (source.length() / Integer.MAX_VALUE);

        try(RandomAccessFile raf = new RandomAccessFile(source, "r")){
            for(int i = 0; i < numBuffers; i++){
                long position = i * Integer.MAX_VALUE;
                int length;
                long chunk = (i + 1) * (long)Integer.MAX_VALUE;
                if(chunk > raf.length()){
                    length = (int)(raf.length() % Integer.MAX_VALUE);
                } else {
                    length = Integer.MAX_VALUE;
                }
                buf = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, length);
                list.add(buf);
            }
        }
        return list;
    }

    private List<MappedByteBuffer> resultBuffers (String[] args,List<MappedByteBuffer> sources) throws IOException {
        List<MappedByteBuffer> results = new ArrayList<>(sources.size());
        Path file = Files.createFile(Paths.get(args[1])).toAbsolutePath();
        try(FileChannel fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)){
            MappedByteBuffer r;
            for(int i = 0; i < sources.size(); i++){
                long position = i * (long)Integer.MAX_VALUE;
                if(sources.get(i).capacity() < Integer.MAX_VALUE){
                    r = fc.map(FileChannel.MapMode.READ_WRITE, position, sources.get(i).capacity() + 2);
                } else {
                    r = fc.map(FileChannel.MapMode.READ_WRITE, position, sources.get(i).capacity());
                }
                results.add(r);
            }
        }
        return results;
    }

    public Path dispatcher(String[] args, File reference, Packer p) throws IOException {
        List<MappedByteBuffer> sources = sourceBuffers(new File(args[0]));
        Path result = Files.createFile(Paths.get(args[1])).toAbsolutePath();
        byte[] separator = System.getProperty("line.separator").getBytes(args[2]);
        try(FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ);
            FileChannel fcResult = FileChannel.open(result, StandardOpenOption.READ, StandardOpenOption.WRITE)){

            byte[] buf = new byte[10000];   //TODO change size to the length of longest line (last long in reference file)
            long position;
            int length;

            MappedByteBuffer src;
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_ONLY, 0, fcRef.size());

            while(ref.hasRemaining()){
                long value = ref.getLong();
                position = p.getPosition(value);
                int relativePosition = (int)(position % Integer.MAX_VALUE);
                length = p.getLength(value);
                int ordinal = (int)(position / Integer.MAX_VALUE);
                src = sources.get(ordinal);
                if((position + length) > (Integer.MAX_VALUE * (ordinal+1))){
                    src.position(relativePosition);
                    int firstChunk = Integer.MAX_VALUE - length;
                    int secondChunk = length - firstChunk;
                    //First part reading & writing
                    src.get(buf, 0, firstChunk);
                    ByteBuffer bb = ByteBuffer.wrap(buf, 0, firstChunk);
                    fcResult.write(bb);
                    //Second part reading & writing
                    src = sources.get(ordinal + 1);
                    src.position(0);
                    src.get(buf, 0, secondChunk);
                    bb = ByteBuffer.wrap(buf, 0, secondChunk);
                    bb.compact();
                    bb.put(separator);
                    bb.flip();
                    fcResult.write(bb);
                } else {
                    src.position(relativePosition);
                    src.get(buf, 0, length);
                    ByteBuffer bb = ByteBuffer.wrap(buf, 0, length);
                    bb.compact();
                    bb.put(separator);
                    bb.flip();
                    fcResult.write(bb);
                }
            }
        }
        return result;
    }

    public void dis2(String[] args, File reference, Packer p) throws IOException {
        List<MappedByteBuffer> sources = sourceBuffers(new File(args[0]));
        List<MappedByteBuffer> results = resultBuffers(args, sources);
        byte[] separator = System.getProperty("line.separator").getBytes(args[2]);
        try(FileChannel fcRef = FileChannel.open(reference.toPath(), StandardOpenOption.READ)){

            byte[] buf = new byte[10000];   //TODO change size to the length of longest line (last long in reference file)
            long position;
            int length;

            MappedByteBuffer src;
            MappedByteBuffer res = results.get(0);
            MappedByteBuffer ref = fcRef.map(FileChannel.MapMode.READ_ONLY, 0, fcRef.size());

            while(ref.hasRemaining()){
                long value = ref.getLong();
                position = p.getPosition(value);
                int relativePosition = (int)(position % (long)Integer.MAX_VALUE);
                length = p.getLength(value);
                int ordinal = (int)(position / Integer.MAX_VALUE);
                src = sources.get(ordinal);

                if(((long)relativePosition + (long)length) > src.limit()){
                    src.position(relativePosition);
                    int firstChunk = src.limit() - relativePosition;
                    int secondChunk = length - firstChunk;
                    //first chunk read & writing
                    src.get(buf, 0, firstChunk);
                    res = putToResultBuffer(results, res, buf, firstChunk);
                    //second chunk read & writing
                    src = sources.get(sources.indexOf(src) + 1);
                    src.position(0);
                    src.get(buf, 0, secondChunk);
                    res = putToResultBuffer(results, res, buf, secondChunk);
                    res = putToResultBuffer(results, res, separator, separator.length);
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

    private MappedByteBuffer putToResultBuffer(List<MappedByteBuffer> results, MappedByteBuffer current, byte[] buffer, int length){
        if(current.position() + (long)length > current.limit()){
            int firstChunk = current.limit() - current.position();
            current.put(buffer, 0, firstChunk);
            current = results.get(results.indexOf(current) + 1);
            current.position(0);
            current.put(buffer, firstChunk, length - firstChunk);
        } else {
            current.put(buffer, 0, length);
        }
        return current;
    }
}
