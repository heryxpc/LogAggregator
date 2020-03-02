package io.heryxpc.logaggregator.util;

public interface CommonConstants {

    int MAXSIZE_READING_QUEUE = 32000;

    int MAXSIZE_WRITING_QUEUE = 128000;

    long MAX_FILE_SIZE = Double.valueOf(512L * 1e9 * Byte.SIZE ).longValue();

    int MAX_LINE_SIZE = 1000;

    int MAX_LINES = 100000;

    long MAX_CHUNK_SIZE = MAX_FILE_SIZE / 1024 * 1024 * Byte.SIZE;
}
