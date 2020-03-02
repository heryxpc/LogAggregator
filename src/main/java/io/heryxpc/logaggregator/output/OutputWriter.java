package io.heryxpc.logaggregator.output;

public interface OutputWriter {

    void write(String message, int lines);
}
