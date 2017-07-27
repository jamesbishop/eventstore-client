package uk.co.blackcell.eventsourcing.api;

import uk.co.blackcell.eventsourcing.api.Event;

public interface EventDeserialiser {
    Event deserialise(final String data, final String eventType);
}