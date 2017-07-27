package uk.co.blackcell.eventsourcing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action3;
import rx.functions.Func1;
import uk.co.blackcell.eventsourcing.api.Aggregate;
import uk.co.blackcell.eventsourcing.api.Command;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventStream;
import uk.co.blackcell.eventsourcing.api.ReflectionUtil;

import java.util.List;

public class AggregateCommandHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandHandler.class);

    private final String streamPrefix;
    private final Func1<String, EventStream> loader;
    private final Action3<String, Long, List<Event>> writer;

    public AggregateCommandHandler(String streamPrefix,
                             Func1<String, EventStream> loader,
                             Action3<String, Long, List<Event>> writer) {
        this.streamPrefix = streamPrefix;
        this.loader = loader;
        this.writer = writer;
    }

    public void handle(Aggregate aggregate, Command command) throws Exception {
        String streamName = String.format("%s-%s", new Object[]{this.streamPrefix, command.aggregateId().toString()});
        this.handle(aggregate, command, streamName);
    }

    public void handle(Aggregate aggregate, Command command, String streamName) throws Exception {
        EventStream eventStream = this.loader.call(streamName);

        for (Object event : eventStream) {
            ReflectionUtil.invokeHandleMethod(aggregate, event);
        }

        ReflectionUtil.invokeHandleMethod(aggregate, command);
        List events1 = aggregate.getUncommittedEvents();
        if(events1 != null && events1.size() > 0) {
            this.writer.call(streamName, eventStream.version(), events1);
        } else {
            LOGGER.debug("No events raised by aggregate");
        }

    }
}