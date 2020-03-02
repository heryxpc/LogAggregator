package io.heryxpc.logaggregator.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileUtils {

    public static File[] accessDirectory(final String logsDirectory)  {
        final File directory = new File(logsDirectory);
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException(String.format("Directory [%s] with logs was not found", logsDirectory));
        }
        File[] files = directory.listFiles();
        if (files.length == 0) {
            throw new IllegalArgumentException(String.format("Directory [%s] is empty", logsDirectory));
        }
        return files;
    }

    public static List<File> accessFiles(File[] files) {
        return Arrays.stream(files).collect(Collectors.toList());
    }
}
