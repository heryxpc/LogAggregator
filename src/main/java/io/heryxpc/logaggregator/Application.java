package io.heryxpc.logaggregator;

import io.heryxpc.logaggregator.manager.EventsManager;
import io.heryxpc.logaggregator.manager.FilesManager;
import io.heryxpc.logaggregator.manager.QueueManager;
import io.heryxpc.logaggregator.model.LogEvent;
import io.heryxpc.logaggregator.output.ConsoleOutputWriter;
import io.heryxpc.logaggregator.output.OutputWriter;
import io.heryxpc.logaggregator.util.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Application {

    private static Logger log = LoggerFactory.getLogger(Application.class);

    private FilesManager filesManager;
    private QueueManager queueManager;
    private EventsManager eventsManager;
    private ArrayBlockingQueue<LogEvent> readingQueue;
    private LinkedBlockingQueue<LogEvent> writingQueue;

    public Application(int readingQueueSize, int writingQueueSize) {
        this.readingQueue = new ArrayBlockingQueue<LogEvent>(readingQueueSize);
        this.writingQueue = new LinkedBlockingQueue<LogEvent>(writingQueueSize);
        final OutputWriter writer = new ConsoleOutputWriter();
        this.eventsManager = new EventsManager(writingQueue, writer);
        this.filesManager = new FilesManager(readingQueue);
        this.queueManager = new QueueManager(readingQueue, writingQueue);
        filesManager.subscribe(queueManager);
        queueManager.subscribe(eventsManager);
    }

    public void process(String logsDir) {
        filesManager.process(logsDir);
    }

    public static void main(String[] args) {
        log.info("Initializing Log Aggregator");
        final Application application = new Application(CommonConstants.MAXSIZE_READING_QUEUE, CommonConstants.MAXSIZE_WRITING_QUEUE);
        String logsDir = "sampleData";
        log.info("Processing server events from directory: {}", logsDir);
        application.process(logsDir);
        log.info("All log events processed from: {}", logsDir);
    }

}
