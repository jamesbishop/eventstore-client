package uk.co.blackcell.eventsourcing.projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import uk.co.blackcell.eventsourcing.api.EventDeserialiser;
import uk.co.blackcell.eventsourcing.api.EventProcessor;
import uk.co.blackcell.eventsourcing.configuration.EventStoreConfiguration;
import uk.co.blackcell.eventsourcing.impl.DefaultEventDeserialiser;
import uk.co.blackcell.eventsourcing.store.http.reader.HttpEventStoreReader;

import java.io.IOException;
import java.util.function.Supplier;

/**
 *
 * This class coordinates all the pieces required to do a projection.  It takes a passed in EventProcessor and
 * invokes a method on that to process a single event at a time for all the events it retrieves from the
 * event store.
 */
public class ProjectionProcessor {

    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionProcessor.class);

    private final EventStoreConfiguration configuration;
    private final EventProcessor eventProcessor;
    private EventDeserialiser eventDeserialiser;
    private HttpEventStoreReader categoryStream;

    /**
     * Constructor.
     *
     * @param configuration  the configuration information
     * @param eventProcessor implementation of EventProcessor
     */
    public ProjectionProcessor(final EventStoreConfiguration configuration,
                               final EventProcessor eventProcessor) {
        this(configuration, eventProcessor, new DefaultEventDeserialiser());
    }

    /**
     * Constructor.
     *
     * @param configuration  the configuration information
     * @param eventProcessor implementation of EventProcessor
     */
    public ProjectionProcessor(final EventStoreConfiguration configuration,
                               final EventProcessor eventProcessor,
                               final EventDeserialiser eventDeserialiser) {
        this.configuration = configuration;
        this.eventProcessor = eventProcessor;
        this.eventDeserialiser = eventDeserialiser;
    }

    /**
     * Subscribes to all events in a stream from a saved starting point.
     */
    public void projectEvents(Supplier<Integer> getNextVersionNumber) {

        LOGGER.info("Projecting events from {}", configuration.getProjectionConfiguration().getStream());

        try {
            categoryStream = new HttpEventStoreReader(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Integer nextVersionNumber = getNextVersionNumber(getNextVersionNumber);

        categoryStream.readStreamEventsForward(
                configuration.getProjectionConfiguration().getStream(),
                nextVersionNumber,
                configuration.getProjectionConfiguration().getPageSize(),
                configuration.getProjectionConfiguration().isKeepAlive()).retryWhen(errors -> errors.flatMap(error -> {
                    LOGGER.error("{}.  Retrying in {} seconds.",
                            error.getMessage(),
                            configuration.getProjectionConfiguration().getSecondsBeforeRetry());
                    int secs = configuration.getProjectionConfiguration().getSecondsBeforeRetry();
                    try { Thread.sleep(secs * 1000); }
                    catch ( Throwable t ) { /* ignore */ }
                    finally { return Observable.just(null); }
                })).subscribe(
                    (event) -> eventProcessor.processEvent(event),
                    (error) -> LOGGER.error(error.getMessage(), error),
                    () -> LOGGER.debug("Projection finished")
        );
    }


    private Integer getNextVersionNumber(Supplier<Integer> getNextVersionNumber) {
        LOGGER.info("Getting next version number to process from data store");
        Integer nextVersionNumber = getNextVersionNumber.get();
        LOGGER.info("Next version number to process is {}", nextVersionNumber);
        return nextVersionNumber;
    }

    public void shutdown() {
        this.categoryStream.shutdown();
    }
}
