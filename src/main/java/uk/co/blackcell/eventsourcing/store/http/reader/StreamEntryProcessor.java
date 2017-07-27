package uk.co.blackcell.eventsourcing.store.http.reader;

import uk.co.blackcell.eventsourcing.api.EntryProcessor;
import uk.co.blackcell.eventsourcing.api.ReadDirection;
import uk.co.blackcell.eventsourcing.api.Take;
import uk.co.blackcell.eventsourcing.store.http.entity.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.util.List;
import java.util.stream.Stream;

public class StreamEntryProcessor implements EntryProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamEntryProcessor.class);

    /**
     * If an event stream is hard deleted then the event type is labelled "$metadata".  Ensure
     * these events are not processed.
     */
    private static final String HARD_DELETED_EVENT_TYPE = "$metadata";

    public void provideEntriesToSubscriber(final List<Entry> entries,
                                           final Subscriber subscriber,
                                           final Take take,
                                           final ReadDirection readDirection) {
        Stream<Entry> entriesToProcess = entries.stream()
                .filter(this::validEvent)
                .sorted((o1, o2) ->
                        readDirection == ReadDirection.FORWARD
                                ? o1.getPositionEventNumber() - o2.getPositionEventNumber()
                                : o2.getPositionEventNumber() - o1.getPositionEventNumber());

        if (take == Take.ONE)
            entriesToProcess
                    .limit(1)
                    .forEach(event -> {
                        LOGGER.debug("Calling subscriber.onNext with event number " + event.getPositionEventNumber());
                        subscriber.onNext(event);
                    });
        else
            entriesToProcess
                    .forEach(event -> {
                        LOGGER.debug("Calling subscriber.onNext with event number " + event.getPositionEventNumber());
                        subscriber.onNext(event);
                    });
    }

    private boolean validEvent(final Entry entry) {
        return entry != null &&
                entry.getEventNumber() != null &&
                entry.getPositionEventNumber() != null &&
                entry.getEventType() != null &&
                !entry.getEventType().equals(HARD_DELETED_EVENT_TYPE);
    }
}
