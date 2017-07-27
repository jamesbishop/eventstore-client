package uk.co.blackcell.eventsourcing.api;

public interface EventProcessor<T> {
    void processEvent(final T event);
}
