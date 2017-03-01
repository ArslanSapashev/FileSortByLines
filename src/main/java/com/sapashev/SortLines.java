package com.sapashev;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Sorts file by lines length.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class SortLines {

    public static void main (String[] args) throws Exception {
        long start = System.currentTimeMillis();
        new SortLines().start(args);
        System.out.println(System.currentTimeMillis() - start);
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
        long[] metas;
        Reader reader = new Reader();
        Sorter sorter = new Sorter();
        Packer packer = new Packer(Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        Combiner combiner = new Combiner();
        List<File> temps = new ArrayList<>();
        Counter counter = new Counter();
        Position position = new Position();
        Copier copier = new Copier();

        try(BufferedReader br = new BufferedReader(isr)){
            while (!isEOF){
                List<Line> lines = new ArrayList<>(bufferSize);
                isEOF = reader.readFromFileTo(lines, charSize, bufferSize, br, counter, position);
                metas = sorter.sort(lines, packer);
                temps.add(combiner.saveToTempFile(metas));
            }
        }
        temps.forEach(File::deleteOnExit);
        File result = createResultFile(temps, packer);
        //copier.directCopy(args, new File(args[0]), result, packer);
        //copier.dispatcher(args, result, packer);
        copier.dis2(args, result, packer);
        result.deleteOnExit();
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

    /**
     * Temps list will be divided to the some sublists according to the concurrency level.
     * After that, each thread applies merge sort to the each sublist concurrently.
     * Resulting sublists will be megre sorted to the final file.
     * @param temps - list of temp files to combine.
     * @param p - packer object which packs and unpacks pairs (position:line length)from packed long value.
     * @return - final combined file, which contains sorted long values from all other temp files.
     * @throws IOException
     */
    private File createResultFile (List<File> temps, Packer p) throws IOException, InterruptedException, ExecutionException {
        File f;
        if(temps.size() >= 4){
            int concurrencyLevel = Runtime.getRuntime().availableProcessors() * 3;
            ExecutorService service = Executors.newFixedThreadPool(concurrencyLevel);
            List<List<File>> files = splitList(temps, concurrencyLevel);
            List<Future<File>> total = runParallelReducing(p, service, files);
            List<File> results = resultParallelReducing(total);
            f = reduce(results, p);
            service.shutdownNow();
        } else {
            f = reduce(temps, p);
        }
        temps.forEach(File::delete);
        return f;
    }

    /**
     * Creates list of resulting files retrieved from each thread.
     * This resulting list will be reduced on a final phase.
     * @param total - futures returned by ExecutorService for each thread result.
     * @return list of resulting files.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private List<File> resultParallelReducing (List<Future<File>> total) throws InterruptedException, ExecutionException {
        List<File> results = new ArrayList<>();
        for(Future<File> fut : total){
            results.add(fut.get());
        }
        return results;
    }

    /**
     * Runs concurrent processing (merge sort and file reducing) of each sublist.
     * @param p - packer to pack/unpack long values.
     * @param service - executor service to run all threads.
     * @return - list of future objects which will return result of concurrent processing of temp files.
     */
    private List<Future<File>> runParallelReducing (Packer p, ExecutorService service, List<List<File>> files) {
        List<Future<File>> listOfList = new ArrayList<>();
        for (List<File> l : files){
            listOfList.add(service.submit(new Reducer(l, p)));
        }
        return listOfList;
    }

    /**
     * Splits raw list to the sublists according to concurrency level.
     * @param temps - raw list (list of temp files to merge).
     * @param concurrencyLevel - how much threads will be process sublists concurrently.
     * @return list of lists of temp files.
     */
    private List<List<File>> splitList (List<File> temps, int concurrencyLevel) {
        int num = temps.size() / concurrencyLevel;
        List<List<File>> files = new ArrayList<>(concurrencyLevel);
        for(int i = 0; i < concurrencyLevel; i++){
            List<File> list = new ArrayList<>(num);
            int y = 0;
            while (y < num && !temps.isEmpty()){
                list.add(temps.remove(0));
                y++;
            }
            files.add(list);
        }
        if(!temps.isEmpty()){
            while (!temps.isEmpty()){
                files.get(files.size() - 1).add(temps.remove(0));
            }
        }
        return files;
    }

    /**
     * Implements Callable to sort'n'merge temporary files.
     */
    private class Reducer implements Callable<File> {
        private final List<File> list;
        private final Packer p;

        public Reducer(List<File> list, Packer p){
            this.list = list;
            this.p = p;
        }

        @Override
        public File call () throws Exception {
            return reduce(list, p);
        }
    }
}
