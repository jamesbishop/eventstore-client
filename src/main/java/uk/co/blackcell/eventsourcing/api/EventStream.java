package uk.co.blackcell.eventsourcing.api;

public interface EventStream extends Iterable<Event> {
	Long version();
}

