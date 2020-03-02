package io.heryxpc.logaggregator.manager;

import io.heryxpc.logaggregator.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static java.time.Instant.ofEpochMilli;
import static java.lang.String.format;

public class QueueManager implements Publisher, Subscriber {

    private static Logger log = LoggerFactory.getLogger(QueueManager.class);

    private Map<Subscriber, LinkedBlockingQueue<LogEvent>> writingSubscribers;
    private ArrayBlockingQueue<LogEvent> processingEvents;
    private LinkedBlockingQueue<LogEvent> writingEvents;
    private List<LogEvent> buffer;
    private ExecutorService internalUpdatePool;
    // Watermark is the lowest event time on the writing queue
    // In case an event comes with timestamp previous to the watermark,
    // will be discarded since events from that time have already being move to the processing queue
    private Instant watermark;
    // The strategy to calculate the watermark is based on a maximum delay that events can take and is a static strict value
    public static final long DELAY_MILLIS = 1000L;

    private static final long WRITING_TIMEOUT_MILLIS = 100L;

    private static final Duration DELAY = Duration.ofMillis(DELAY_MILLIS);

    private static final int QUEUE_MGR_THREADS = 1;

    public QueueManager(ArrayBlockingQueue<LogEvent> processingEvents, LinkedBlockingQueue<LogEvent> writingEvents) {
        this.processingEvents = processingEvents;
        this.writingEvents = writingEvents;
        initialize();
    }

    public void initialize() {
        writingSubscribers = new HashMap<Subscriber, LinkedBlockingQueue<LogEvent>>();
        watermark = ofEpochMilli(0);
        buffer = new LinkedList<LogEvent>();
        internalUpdatePool = Executors.newFixedThreadPool(QUEUE_MGR_THREADS);
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        writingSubscribers.put(subscriber, writingEvents);
    }

    @Override
    public void unsubscribe(Subscriber subscriber) {
        writingSubscribers.remove(subscriber);

    }

    @Override
    public void notifySubscribers() {
        internalNotifySubscribers(writingSubscribers);
    }

    protected void internalNotifySubscribers(Map<Subscriber, LinkedBlockingQueue<LogEvent>> writingSubscribers) {
        writingSubscribers.forEach( (subscriber, logEvents) -> subscriber.update(logEvents));
    }

    @Override
    public void update(AbstractQueue<LogEvent> events) {
        internalUpdatePool.execute(() -> internalUpdate((ArrayBlockingQueue<LogEvent>) events));
    }

    protected void internalUpdate(ArrayBlockingQueue<LogEvent> events) {
        if (events != processingEvents) {
            throw new IllegalArgumentException(format("Received queue of events is different to the processingEvents queue"));
        }
        final Instant readyToGo = watermark.plus(DELAY);
        while (!buffer.isEmpty()) {
            if (Instant.now().isAfter(readyToGo)) {
                flush();
                notifySubscribers();
                break;
            } else {
                try {
                    wait(WRITING_TIMEOUT_MILLIS);
                } catch (InterruptedException e) {
                    log.error("Error waiting the buffer {} to flush", buffer);
                }
            }
        }
    }

    private void flush() {
        // Must include the watermark validation to discard any already processed timestamp
        final Instant prevWatermark = watermark;
        processingEvents.removeIf(logEvent -> ofEpochMilli(logEvent.getTimestamp()).isBefore(watermark));
        processingEvents.drainTo(buffer);
        Collections.sort(buffer, Collections.reverseOrder());  // We need reverse order, since we are taking the first elements
        try {
            while (!buffer.isEmpty()) {
                final LogEvent event = ((Queue<LogEvent>)buffer).poll();
                boolean isFlush = writingEvents.offer(event, WRITING_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (!isFlush) {
                    throw new TimeoutException(String.format("Writing to processing queue %s timed out. Latest event to write was %s", writingEvents, event));
                }
                Optional<LogEvent> writingTail = Optional.ofNullable(writingEvents.peek());
                watermark = ofEpochMilli(writingTail.orElse(event).getTimestamp());
            }
        } catch (InterruptedException | TimeoutException e) {
            log.error("Sync error flushing the buffer {} to processing queue {}", buffer, processingEvents, e);
        } finally {
            if (watermark.isBefore(prevWatermark)) {
                watermark = prevWatermark.plus(DELAY);
            }
        }
    }

    @Override
    public AbstractQueue<LogEvent> getSubscribedQueue() {
        return processingEvents;
    }
}
