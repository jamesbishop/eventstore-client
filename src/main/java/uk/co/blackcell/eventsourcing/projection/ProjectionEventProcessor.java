package uk.co.blackcell.eventsourcing.projection;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventDeserialiser;
import uk.co.blackcell.eventsourcing.api.EventProcessor;
import uk.co.blackcell.eventsourcing.store.http.entity.Entry;

import java.util.UUID;
import java.util.function.Function;

/**
 * This can be used as the projection processor for all projections.
 * <p>
 * NOTE: Currently the Dropwizard projection bundle uses reflection rather than suppliers
 * so a sub class is required rather than direct use of this class.
 *
 * @param <ET> The Entity Type
 */
@Data
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ProjectionEventProcessor<ET> implements EventProcessor<Entry> {

    @FunctionalInterface
    public interface EntitySupplier<ET> {
        ET get(UUID id);
    }

    @FunctionalInterface
    public interface EntityPersister<ET> {
        void save(ET entity, Integer version);
    }

    @FunctionalInterface
    public interface EntityDeleter<ET> {
        void delete(ET entity);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProjectionEventProcessor.class);

    private final EntitySupplier<ET> entitySupplier;
    private final EntityPersister<ET> entityPersister;
    private final EntityDeleter<ET> entityDeleter;
    private final EventDeserialiser eventDeserialiser;
    private final Function<ET, EventHandler<?,ET>> eventHandlerSupplier;
    private final EventProcessor<Entry> preProcessor;

    @Override
    public void processEvent(Entry entry) {

        if (null != preProcessor) {
            LOG.debug("Pre-Processing event {}.", entry.getEventType());
            preProcessor.processEvent(entry);
        }

        LOG.debug("Processing event {}.", entry.getEventType());


        // Extract the event
        Event myEvent = eventDeserialiser.deserialise(entry.getData(), entry.getEventType());

        ET entity = entitySupplier.get(myEvent.aggregateId());

        EventHandler<?,ET> projection = eventHandlerSupplier.apply(entity);
        projection.handle(myEvent);

        if (entity == null && projection.getEntity() == null) {
            LOG.debug("Null entity");
        }
        else if (projection.getEntity() != null) {
            entityPersister.save(projection.getEntity(),entry.getPositionEventNumber());
        }
        else if (entity != null && projection.getEntity() == null) {
            // Remove entity - Dirty dirty fix by PvdM - TODO
            entityDeleter.delete(entity);
        }

        LOG.debug("Processed {} event {}", entry.getEventType(), myEvent);

    }

}
