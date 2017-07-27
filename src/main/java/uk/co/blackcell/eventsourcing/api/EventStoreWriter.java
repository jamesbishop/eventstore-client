package uk.co.blackcell.eventsourcing.api;

import java.util.List;

import uk.co.blackcell.eventsourcing.api.Event;

public interface EventStoreWriter {
    void store(final String streamName, final long expectedVersion, final List<Event> events);
    void store(final String streamName, final long expectedVersion, final Event event);
}
