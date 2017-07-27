package uk.co.blackcell.eventsourcing.projection;

import org.junit.Test;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.exception.EventHandlerException;

import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by luke on 10/11/2016.
 */
public class EventHandlerTest {

    public interface DummyEntity { }

    public class EventType1 implements Event {
        @Override
        public UUID aggregateId() {
            return null;
        }
    }
    public class EventType2 implements Event {
        @Override
        public UUID aggregateId() {
            return null;
        }
    }
    public class EventType3 implements Event {
        @Override
        public UUID aggregateId() {
            return null;
        }
    }

    public class TestEventHandler extends EventHandler<TestEventHandler,DummyEntity> {

        final Function<Event,Void> fn;

        public TestEventHandler(Function<Event, Void> fn) {
            this.fn = fn;
        }

        @Override
        public DummyEntity getEntity() {
            return null;
        }

        public void handle(EventType1 ev) { fn.apply(ev); }
        public void handle(EventType2 ev) { fn.apply(ev); }
    }

    private final Function fn = mock(Function.class);
    EventHandler handler = new TestEventHandler(fn);

    @Test
    public void ignoresUnhandledEvent() {

        EventType3 event = new EventType3();
        handler.handle(event);

        verify(fn,never()).apply(event);
    }

    @Test
    public void handlesEvent1() {

        EventType1 event = new EventType1();
        handler.handle(event);

        verify(fn,times(1)).apply(event);
    }

    @Test
    public void handlesEvent2withException() {

        EventType2 event = new EventType2();
        IllegalArgumentException exception = new IllegalArgumentException("my error");
        when(fn.apply(event)).thenThrow(exception);

        try {
            handler.handle(event);
            fail();
        }
        catch ( EventHandlerException exc ) {
            verify(fn,times(1)).apply(event);
            assertThat(exc.getCause(),is(exception));
        }
    }



}
