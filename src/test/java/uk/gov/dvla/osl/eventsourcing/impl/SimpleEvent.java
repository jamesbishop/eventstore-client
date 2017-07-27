package uk.co.blackcell.eventsourcing.impl;

import uk.co.blackcell.eventsourcing.api.Event;

import java.util.UUID;

public class SimpleEvent implements Event {

    private UUID id;

    public SimpleEvent(UUID id) {
        this.id = id;
    }

    @Override
    public UUID aggregateId() {
        return id;
    }
}
