package uk.co.blackcell.eventsourcing.projection;

import org.junit.Test;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventDeserialiser;
import uk.co.blackcell.eventsourcing.api.EventProcessor;
import uk.co.blackcell.eventsourcing.store.http.entity.Entry;

import java.util.UUID;
import java.util.function.Function;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProjectionEventProcessorTest {

    final ProjectionEventProcessor processor = ProjectionEventProcessor.builder()
            .entitySupplier(mock(ProjectionEventProcessor.EntitySupplier.class))
            .entityPersister(mock(ProjectionEventProcessor.EntityPersister.class))
            .eventDeserialiser(mock(EventDeserialiser.class))
            .eventHandlerSupplier((mock(Function.class)))
            .preProcessor(mock(EventProcessor.class))
            .build();

    @Test
    public void process() {
        Event event = mock(Event.class);
        Entry input = mock(Entry.class);
        when(input.getEventType()).thenReturn("test.event.type");
        when(processor.getEventDeserialiser().deserialise(any(),eq("test.event.type"))).thenReturn(event);

        UUID eventId = UUID.randomUUID();
        when(event.aggregateId()).thenReturn(eventId);

        Object entity1 = new Object();
        Object entity2 = new Object();
        when(processor.getEntitySupplier().get(eq(eventId))).thenReturn(entity1);

        EventHandler handler = mock(EventHandler.class);
        when(processor.getEventHandlerSupplier().apply(eq(entity1))).thenReturn(handler);
        when(handler.getEntity()).thenReturn(entity2);

        processor.processEvent(input);

        verify(processor.getPreProcessor()).processEvent(input);
        verify(processor.getEntityPersister()).save(eq(entity2),anyInt());
    }


}
