package com.sapashev;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Sony on 22.02.2017.
 */
public class Copier {

    /**
     * Copies lines from source file to destination one.
     * To the end of each line appends bytes of "line.separator" property
     * @param args - args of program
     * @param result - temporary file which contains sorted long values (defines pair of position:line length)
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value
     * @throws IOException
     */
    public void copyFromSourceToDestination (String[] args, File result, Packer p) throws IOException, ExecutionException, InterruptedException {
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        List<File> files = splitToFiles(result, concurrencyLevel);
        ExecutorService service = Executors.newFixedThreadPool(concurrencyLevel);
        List<Future<Path>> futures = callExecutor(files, args, concurrencyLevel, p, service);
        List<Path> semiResults = retrieveSemiResults(futures);
        service.shutdownNow();
        appendFilesTo(args[1], semiResults);
    }

    private void appendFilesTo (String result, List<Path> files) throws IOException {
        try(FileChannel fcResult = FileChannel.open(Paths.get(result), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)){
            for(Path p : files){
                FileChannel fc = FileChannel.open(p, StandardOpenOption.READ);
                fc.transferTo(0, fc.size(),fcResult);
                fc.close();
            }
        }
    }

    private List<Path> retrieveSemiResults (List<Future<Path>> paths) throws InterruptedException, ExecutionException {
        List<Path> semiResults = new ArrayList<>();
        for(Future<Path> f : paths){
            semiResults.add(f.get());
        }
        semiResults.forEach((x)->x.toFile().deleteOnExit());
        return semiResults;
    }

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

    /**
     * Copies lines from source file to destination one.
     * To the end of each line appends bytes of "line.separator" property
     * @param args - args of program
     * @param reference - temporary file which contains sorted long values (defines pair of position:line length)
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value
     * @throws IOException
     */
    private Path copy (String[] args, File source, File reference, Packer p) throws IOException {
        File result = File.createTempFile("copier_", "ars");
        try (final RandomAccessFile raf = new RandomAccessFile(source, "r");
             final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(result));
             final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(reference)))) {

            final int LONG_SIZE = 8;
            long l;
            int numBytes;
            long position;
            int charSize = Charset.forName(args[2]).encode("s").limit();
            byte[] separator = Charset.forName(args[2]).encode(System.getProperty("line.separator")).array();

            long limit = reference.length() / LONG_SIZE;
            byte[] buffer = new byte[10000];
            int readBytes = 0;
            for(int i = 0; i < limit; i++){
                l = dis.readLong();
                numBytes = p.getLength(l) * charSize;
                position = p.getPosition(l);
                raf.seek(position);
                if ((readBytes = raf.read(buffer, 0, numBytes)) == -1) {
                    throw new EOFException(String.format("Unexpected end of file at position %s", position));
                }
                bos.write(buffer, 0, readBytes);
                bos.write(separator);
            }
        }
        return Paths.get(result.toString());
    }

    private List<Future<Path>> callExecutor(List<File> files, String[]args, int concurrencyLevel, Packer p, ExecutorService service) throws IOException, ExecutionException, InterruptedException {
        List<Future<Path>> paths = new ArrayList<>();
        for(File f : files){
            paths.add(service.submit(new ChunkCopier(args, new File(args[0]), f, p)));
        }
        return paths;
    }

    /**
     * Splits result file to the sub files, to process them concurrently later.
     * @param toSplit - result file to split.
     * @param concurrencyLevel - number of sub files to create.
     * @return - list of sub files.
     * @throws IOException
     */
    private List<File> splitToFiles (File toSplit, int concurrencyLevel) throws IOException {
        List<File> files = new ArrayList<>(concurrencyLevel);
        int chunkSize = (int)(toSplit.length() / concurrencyLevel);
        try(RandomAccessFile raf = new RandomAccessFile(toSplit, "r")){
            long readBytes = 0;
            for(int i = 0; i < concurrencyLevel; i++){
                File f = File.createTempFile("copier_", "ars");
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
                byte[] buffer = new byte[chunkSize];
                int count = raf.read(buffer);
                if(count > 0){
                    dos.write(buffer);
                }
                readBytes += count;
                dos.flush();
                dos.close();
                files.add(f);
            }
            if(readBytes < raf.length()){
                File f = File.createTempFile("copier_", "ars");
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
                byte[] buffer = new byte[(int)(raf.length() - readBytes)];
                if(raf.read(buffer) > 0){
                    dos.write(buffer);
                }
                dos.flush();
                dos.close();
                files.add(f);
            }
            files.forEach(File::deleteOnExit);
        }
        return files;
    }

    private class ChunkCopier implements Callable<Path> {
        private File source;
        private File reference;
        private Packer p;
        private String[] args;

        public ChunkCopier(String[] args, File source, File reference, Packer p){
            this.source = source;
            this.reference = reference;
            this.args = args;
            this.p = p;
        }

        @Override
        public Path call() throws IOException {
            return copy(args, source, reference, p);
        }
    }
}
