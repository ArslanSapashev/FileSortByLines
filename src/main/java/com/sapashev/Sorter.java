package com.sapashev;

/**
 * Sorts array by quick sort algorithm.
 * @author Arslan Sapashev
 * @since 28.12.2016
 * @version 1.0
 */
public class Sorter {
    public void quickSort (long[] metas, int start, int end, Packer p) {
        if (start < end){
            int leftPointer = start;
            int rightPointer = end;
            int middle = leftPointer - (leftPointer - rightPointer) / 2;
            int middleValue = p.getLength(metas[middle]);
            while (leftPointer < rightPointer) {
                while (leftPointer < middle && (p.getLength(metas[leftPointer]) <= middleValue)) {
                    leftPointer++;
                }
                while (rightPointer > middle && (middleValue) <= p.getLength(metas[rightPointer])) {
                    rightPointer--;
                }
                if (leftPointer < rightPointer) {
                    swap(metas, leftPointer, rightPointer);
                    if (leftPointer == middle)
                        middle = rightPointer;
                    else if (rightPointer == middle)
                        middle = leftPointer;
                }
            }
            quickSort(metas, start, middle, p);
            quickSort(metas, middle+1, end, p);
        }
    }

    /**
     * Swaps (changes) position of two values in array.
     * @param metas - source array.
     * @param i - first value.
     * @param j - second value;
     */
    private void swap (long[] metas, int i, int j) {
        long temp = metas[i];
        metas[i] = metas[j];
        metas[j] = temp;
    }
}