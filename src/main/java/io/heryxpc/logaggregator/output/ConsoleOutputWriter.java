package io.heryxpc.logaggregator.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static java.lang.System.lineSeparator;
import static java.lang.System.out;

public class ConsoleOutputWriter implements OutputWriter {

    Logger log = LoggerFactory.getLogger(ConsoleOutputWriter.class);

    private static char LF = lineSeparator().charAt(0);

    private ExecutorService consoleUpdatePool;

    public ConsoleOutputWriter() {
        consoleUpdatePool = Executors.newSingleThreadExecutor();
    }

    public ConsoleOutputWriter(ExecutorService consoleUpdatePool) {
        this.consoleUpdatePool = consoleUpdatePool;
    }

    @Override
    public void write(String message, int lines) {
        try {
            int linesDone = writeAsync(message).get();
            if (linesDone != lines) {
                log.warn("The number of lines written vs events received was: Expected [{}] Actual [[]]", lines, linesDone);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error writing to console Asynchronously", e);
        }
    }

    public Future<Integer> writeAsync(final String message) throws InterruptedException {
        CompletableFuture<Integer> writeComplete = new CompletableFuture<Integer>();
        consoleUpdatePool.submit(() -> {
            int count = 0;
            out.print(message);
            if (message.charAt(message.length() - 1) != LF) {
                out.println();
                count++;
            }
            count += message.split(lineSeparator()).length;
            writeComplete.complete(count);
        });
        return writeComplete;
    }
}
