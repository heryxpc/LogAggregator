package io.heryxpc.logaggregator.manager;

import io.heryxpc.logaggregator.model.LogEvent;

import java.util.AbstractQueue;


public interface Subscriber {

    void update(AbstractQueue<LogEvent> events);
    AbstractQueue<LogEvent> getSubscribedQueue();

}
