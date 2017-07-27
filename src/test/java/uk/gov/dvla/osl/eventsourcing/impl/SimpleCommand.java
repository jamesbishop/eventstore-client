package uk.co.blackcell.eventsourcing.impl;

import uk.co.blackcell.eventsourcing.api.Command;

import java.util.UUID;

public class SimpleCommand implements Command {

    private UUID id;

    public SimpleCommand(UUID id) {
        this.id = id;
    }

    @Override
    public UUID aggregateId() {
        return id;
    }
}