package io.heryxpc.logaggregator.model;

import java.io.File;
import java.io.RandomAccessFile;

public class Chunk {

    private RandomAccessFile origin;
    private long size;
    private long index;

    public Chunk(RandomAccessFile origin, long size, long index) {
        this.origin = origin;
        this.size = size;
        this.index = index;
    }

    public RandomAccessFile getOrigin() {
        return origin;
    }

    public void setOrigin(RandomAccessFile origin) {
        this.origin = origin;
    }

    public long getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
