package uk.co.blackcell.eventsourcing.exception;

/**
 * Unexpected technical exception has occured within EventStore Client lib.
 *
 *  TODO : We need to refactor the client lib to handle EXCEPTIONS !!
 *       this is a major refactor job.
 *       for this sprint we are throwing a runtime exception  - MS
 *
 */

public class EventStoreClientTechnicalException extends RuntimeException {
    public EventStoreClientTechnicalException(final String message) {
        super(message);
    }
    public EventStoreClientTechnicalException(final String message, Throwable throwable) {
        super(message, throwable);
    }
}
