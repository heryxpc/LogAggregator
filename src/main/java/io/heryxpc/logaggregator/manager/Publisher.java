package io.heryxpc.logaggregator.manager;

public interface Publisher {

    void subscribe(Subscriber subscriber);
    void unsubscribe(Subscriber subscriber);
    void notifySubscribers();
}
