package com.sapashev;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sorts array by quick sort algorithm.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class Sorter {

    public long[] quik2 (List<Line> lines, Packer packer){
        List<Line> result = lines.parallelStream().sorted(Comparator.comparing(Line::length)).collect(Collectors.toList());
        long[] metas = new long[result.size()];
        for(int i = 0; i < metas.length; i++){
            metas[i] = packer.packToLong(result.get(i).length(), result.get(i).position());
        }
        return metas;
    }
}