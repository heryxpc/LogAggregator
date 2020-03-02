package io.heryxpc.logaggregator.command;

import io.heryxpc.logaggregator.model.Chunk;
import io.heryxpc.logaggregator.model.CommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class SliceCommand implements Command {

    Logger log = LoggerFactory.getLogger(SliceCommand.class);

    private File logFile;
    private long maxChunkSize;

    public SliceCommand(File logFile, long maxChunkSize) {
        this.logFile = logFile;
        this.maxChunkSize = maxChunkSize;
    }

    protected RandomAccessFile createRandomAccessFile(final File file, long currPos, long length) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(currPos);
        randomAccessFile.setLength(length);
        return randomAccessFile;
    }

    private List<Chunk> sliceInChunks(final File file) throws IOException {
        long fileSize = file.length();
        List<Chunk> chunks = null;
        if (fileSize != 0 || file.exists()) {
            int maxChunks = fileSize / maxChunkSize  < Long.valueOf(Integer.MAX_VALUE) ?
                    (int) (fileSize / maxChunkSize + 1 ):
                    Integer.MAX_VALUE;
            chunks = new ArrayList<Chunk>(maxChunks);
            for (long pos = 0; pos < fileSize; pos+= maxChunkSize) {
                long chunkSize = maxChunkSize;
                if (pos + chunkSize > fileSize) {
                    chunkSize = fileSize - pos;
                }
                long length = chunkSize;
                RandomAccessFile randomAccessFile = createRandomAccessFile(file, pos, length);
                Chunk chunk = new Chunk(randomAccessFile, chunkSize, pos);
                chunks.add(chunk);
            }
        }
        return chunks;
    }

    @Override
    public CommandResult execute() {
        CommandResult result = new CommandResult();
        try {
            List<Chunk> chunks = sliceInChunks(logFile);
            result.setResult(chunks);
        } catch (IOException ioe) {
            log.error("Couldn't create chunks from file {} on path {}", logFile.getName(), logFile.getAbsolutePath(), ioe);
        }
        return result;
    }
}
