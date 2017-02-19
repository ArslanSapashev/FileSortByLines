package com.sapashev;

/**
 * Created by Sony on 20.02.2017.
 */
public class Line {
    private final long position;
    private final int length;

    public Line(long position, int length){
        this.position = position;
        this.length = length;
    }

    public long position(){
        return this.position;
    }

    public int length(){
        return this.length;
    }
}
