package io.heryxpc.logaggregator.model;

public class LogEvent implements Comparable<LogEvent>{
    private long timestamp;
    private String event;

    public LogEvent() {
    }

    public LogEvent(long timestamp, String event) {
        this.timestamp = timestamp;
        this.event = event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public int compareTo(LogEvent logEvent) {
        return Long.compare(this.timestamp, logEvent.timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogEvent) ) {
            return false;
        }
        LogEvent logEvent = (LogEvent)obj;
        if (obj == null ) {
            return false;
        }
        if (this.event == null && logEvent.event != null) {
            return false;
        }
        return this.timestamp == logEvent.timestamp && this.event.equals(logEvent.event);
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "timestamp=" + timestamp +
                ", event='" + event + '\'' +
                '}';
    }
}
