package io.heryxpc.logaggregator.manager;

import io.heryxpc.logaggregator.model.LogEvent;
import io.heryxpc.logaggregator.output.OutputWriter;
import io.heryxpc.logaggregator.util.CommonConstants;

import java.util.AbstractQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static java.time.Instant.ofEpochMilli;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.lang.System.lineSeparator;

public class EventsManager implements Subscriber {

    private LinkedBlockingQueue<LogEvent> writingEvents;
    private OutputWriter consoleOutputWriter;
    private StringBuilder reusableString;
    private StringBuffer stringBuffer;
    private ExecutorService eventsUpdatePool;

    private static final int EVENT_MGR_THREADS = 5;

    private static final int MAX_BUFFER_SIZE = CommonConstants.MAX_LINE_SIZE * CommonConstants.MAX_LINES;

    public EventsManager(LinkedBlockingQueue<LogEvent> writingEvents, OutputWriter consoleOutputWriter) {
        this.writingEvents = writingEvents;
        this.consoleOutputWriter = consoleOutputWriter;
        reusableString = new StringBuilder();
        stringBuffer = new StringBuffer();
        eventsUpdatePool = Executors.newFixedThreadPool(EVENT_MGR_THREADS);
    }

    public String eventToString(LogEvent event) {
        reusableString.setLength(0);
        String iso8601Timestamp = ISO_INSTANT.format(ofEpochMilli(event.getTimestamp()));
        reusableString.append(iso8601Timestamp);
        reusableString.append(',');
        reusableString.append(event);
        return reusableString.toString();
    }

    @Override
    public void update(AbstractQueue<LogEvent> events) {
        eventsUpdatePool.execute(() -> internalUpdate((LinkedBlockingQueue<LogEvent>) events));
    }

    protected void internalUpdate(LinkedBlockingQueue<LogEvent> events) {
        stringBuffer.setLength(MAX_BUFFER_SIZE);
        int numberOfEvents = events.size();
        for (int i = 0; i < numberOfEvents || stringBuffer.capacity() <= MAX_BUFFER_SIZE; i++) {
            LogEvent event = events.poll();
            if (event == null) {
                break;
            }
            stringBuffer.append(eventToString(event));
            stringBuffer.append(lineSeparator());
        }
        String output = stringBuffer.toString();
        stringBuffer.setLength(0);
        consoleOutputWriter.write(output, numberOfEvents);
    }

    @Override
    public AbstractQueue<LogEvent> getSubscribedQueue() {
        return writingEvents;
    }
}
