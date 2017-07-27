package uk.co.blackcell.eventsourcing.api;

import uk.co.blackcell.eventsourcing.store.http.entity.EventStreamData;

import java.io.IOException;

public interface DataFetcher {
    EventStreamData fetchStreamData(final String url, final boolean longPoll) throws IOException;
}
