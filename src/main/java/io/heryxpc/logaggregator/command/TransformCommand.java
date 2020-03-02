package io.heryxpc.logaggregator.command;

import io.heryxpc.logaggregator.model.Chunk;
import io.heryxpc.logaggregator.model.CommandResult;
import io.heryxpc.logaggregator.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TransformCommand implements Command {

    Logger log = LoggerFactory.getLogger(TransformCommand.class);

    private Chunk chunk;

    public TransformCommand(Chunk chunk) {
        this.chunk = chunk;
    }

    private List<LogEvent> transform(final Chunk chunk) throws IOException {
        RandomAccessFile file = chunk.getOrigin();
        long maxPos = chunk.getIndex() + chunk.getSize();
        List<LogEvent> events = new ArrayList<LogEvent>();
        while (file.getFilePointer() < maxPos) {
            String line = file.readLine();
            if (line == null) {
                break;
            }
            LogEvent event = stringToEvent(line);
            events.add(event);
        }
        return events;
    }

    public static LogEvent stringToEvent(String line) {
        String[] lineEntries = line.split(",");
        long timestamp = Instant.parse(lineEntries[0]).toEpochMilli();
        String event = lineEntries[1];
        return new LogEvent(timestamp, event);
    }

    @Override
    public CommandResult execute() {
        CommandResult result = new CommandResult();
        try {
            List<LogEvent> events = transform(chunk);
            result.setResult(events);
        } catch (IOException ioe) {
            log.error("Couldn't read lines from chunk {}", chunk, ioe);
        }
        return result;
    }
}
