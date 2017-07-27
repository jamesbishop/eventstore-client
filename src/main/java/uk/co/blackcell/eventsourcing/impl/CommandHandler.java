package uk.co.blackcell.eventsourcing.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action3;
import rx.functions.Func1;
import uk.co.blackcell.eventsourcing.api.Aggregate;
import uk.co.blackcell.eventsourcing.api.Command;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventStoreReader;
import uk.co.blackcell.eventsourcing.api.EventStoreWriter;
import uk.co.blackcell.eventsourcing.api.EventStream;
import uk.co.blackcell.eventsourcing.api.ReflectionUtil;

import java.util.List;

public class CommandHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandHandler.class);

    private final String streamPrefix;
    private Func1<String, EventStream> loader;
    private Action3<String, Long, List<Event>> writer;
    private final CommandHandlerLookup commandHandlerLookup;
    private ObjectMapper mapper;

    @Deprecated
    public CommandHandler(final EventStoreReader eventStoreReader,
                          final EventStoreWriter eventStoreWriter,
                          final String streamPrefix,
                          final ObjectMapper mapper,
                          final Class<?>... aggregateTypes) {

        this(streamPrefix, eventStoreReader::loadEventStream, eventStoreWriter::store, aggregateTypes);
        this.mapper = mapper;
        mapper.registerModule(new JavaTimeModule());
    }

    public CommandHandler(final String streamPrefix,
                          final Func1<String, EventStream> loader,
                          final Action3<String, Long, List<Event>> writer,
                          final Class<?>... aggregateTypes) {
        this(streamPrefix, loader, writer, new CommandHandlerLookup(ReflectionUtil.HANDLE_METHOD, aggregateTypes));
    }

    public CommandHandler(final String streamPrefix,
                          final Func1<String, EventStream> loader,
                          final Action3<String, Long, List<Event>> writer,
                          CommandHandlerLookup commandHandlerLookup) {
        this.streamPrefix = streamPrefix;
        this.loader = loader;
        this.writer = writer;
        this.commandHandlerLookup = commandHandlerLookup;
    }

    public void handle(final Command command) throws Exception {
        String streamName = String.format("%s-%s", this.streamPrefix, command.aggregateId().toString());
        handle(command, streamName);
    }

    public void handle(final Command command,
                       final String streamName) throws Exception {

        final EventStream eventStream = loader.call(streamName);
        final Object target = commandHandlerLookup.newAggregateInstance(command);
        for (Event event : eventStream) {
            ReflectionUtil.invokeHandleMethod(target, event);
        }
        ReflectionUtil.invokeHandleMethod(target, command);
        final List<Event> events = ((Aggregate) target).getUncommittedEvents();
        if (events != null && events.size() > 0) {
            writer.call(streamName, eventStream.version(), events);
        } else {
            LOGGER.debug("No events raised by aggregate");
        }
    }
}
