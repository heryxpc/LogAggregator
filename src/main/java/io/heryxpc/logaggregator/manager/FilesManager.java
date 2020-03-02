package io.heryxpc.logaggregator.manager;

import io.heryxpc.logaggregator.command.SliceCommand;
import io.heryxpc.logaggregator.command.TransformCommand;
import io.heryxpc.logaggregator.model.Chunk;
import io.heryxpc.logaggregator.model.CommandResult;
import io.heryxpc.logaggregator.model.LogEvent;
import io.heryxpc.logaggregator.util.CommonConstants;
import io.heryxpc.logaggregator.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class FilesManager implements Publisher {

    private static final Logger log = LoggerFactory.getLogger(FilesManager.class);

    private ArrayBlockingQueue<LogEvent> processingEvents;
    private Map<Subscriber, ArrayBlockingQueue<LogEvent>> processingSubscribers;

    public FilesManager(ArrayBlockingQueue<LogEvent> processingEvents) {
        this.processingEvents = processingEvents;
        this.processingSubscribers = new HashMap<Subscriber, ArrayBlockingQueue<LogEvent>>();
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        processingSubscribers.put(subscriber, processingEvents);
    }

    @Override
    public void unsubscribe(Subscriber subscriber) {
        processingSubscribers.remove(subscriber);
    }

    private void internalNotifySubscribers(Map<Subscriber, ArrayBlockingQueue<LogEvent>> processingSubscribers) {
        processingSubscribers.forEach((subscriber, events) -> subscriber.update(events));
    }

    @Override
    public void notifySubscribers() {
        internalNotifySubscribers(processingSubscribers);
    }

    public void process(String dir) {
        List<File> fileList = FileUtils.accessFiles(FileUtils.accessDirectory(dir));
        fileList.parallelStream().forEach(file -> processChunk(file));
    }

    protected void processChunk(final File fromFile) {
        SliceCommand slicer = new SliceCommand(fromFile, CommonConstants.MAX_CHUNK_SIZE);
        CommandResult sliceResult = slicer.execute();
        if (!sliceResult.isEmpty()) {
            List<Chunk> chunksToProcess = (List<Chunk>) sliceResult.getResult();
            chunksToProcess.stream().forEach(chunk -> processEvents(chunk));
        }

    }

    protected void processEvents(final Chunk chunk) {
        TransformCommand transformer = new TransformCommand(chunk);
        CommandResult chunkResult = transformer.execute();
        if (!chunkResult.isEmpty()) {
            List<LogEvent> events = (List<LogEvent>) chunkResult.getResult();
            events.forEach(event -> {
                try {
                    processingEvents.put(event);
                } catch (InterruptedException e) {
                    log.error("Error putting event [{}] while processing Chunk [{}]", event, chunk);
                }
                notifySubscribers();
            });
        }
    }

}
