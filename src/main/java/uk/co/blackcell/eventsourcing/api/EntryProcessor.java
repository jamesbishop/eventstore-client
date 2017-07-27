package uk.co.blackcell.eventsourcing.api;

import uk.co.blackcell.eventsourcing.store.http.entity.Entry;
import rx.Subscriber;

import java.util.List;

public interface EntryProcessor {
    void provideEntriesToSubscriber(final List<Entry> entries,
                                    final Subscriber subscriber,
                                    final Take take,
                                    final ReadDirection readDirection);
}

