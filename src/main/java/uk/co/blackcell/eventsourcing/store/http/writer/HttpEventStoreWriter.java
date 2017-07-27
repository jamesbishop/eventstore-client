package uk.co.blackcell.eventsourcing.store.http.writer;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventStoreWriter;
import uk.co.blackcell.eventsourcing.configuration.EventStoreConfiguration;
import uk.co.blackcell.eventsourcing.exception.EventStoreClientTechnicalException;
import uk.co.blackcell.eventsourcing.store.http.EventStoreService;
import uk.co.blackcell.eventsourcing.store.http.ServiceGenerator;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class HttpEventStoreWriter implements EventStoreWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpEventStoreWriter.class);
    private static final String WRITING_EVENT_ERROR = "Error in Writing event to event store.";

    private final ObjectMapper mapper;
    private final EventStoreService eventService;

    public HttpEventStoreWriter(final EventStoreConfiguration configuration, final ObjectMapper mapper) {
        this.mapper = mapper;
        mapper.registerModule(new JavaTimeModule());
        eventService = ServiceGenerator.createService(EventStoreService.class, configuration);
    }

    @Override
    public void store(final String streamName, final long expectedVersion, final List<Event> events) {

        final List<AddEventRequest> addEventRequests = events.stream()
                .map((event) -> new AddEventRequest(UUID.randomUUID(),
                        event.getClass().getTypeName(),
                        event))
                .collect(Collectors.toList());

        try {

            final String body = mapper.writeValueAsString(addEventRequests);

            LOGGER.debug("Storing events: " + body);

            final Call<Void> call = eventService.postEvents(expectedVersion,
                    streamName,
                    RequestBody.create(MediaType.parse("text/plain"),
                            body));

            final Response<Void> response = call.execute();

            if (!response.isSuccessful()) {
                LOGGER.error("{} Response code: {}. Stream name: {}.", WRITING_EVENT_ERROR, response.code(), streamName);
                if(response.errorBody() != null) {
                    LOGGER.error("Error body is: {}", response.errorBody().string());
                }
                throw new EventStoreClientTechnicalException(WRITING_EVENT_ERROR);
            }
        } catch (IOException e) {
            LOGGER.error("{} Stream name: {}. Error: {}", WRITING_EVENT_ERROR, streamName, e.getMessage());
            throw new RuntimeException(WRITING_EVENT_ERROR);
        }
    }

    @Override
    public void store(final String streamName, final long expectedVersion, final Event event) {
        final List<Event> events = new ArrayList<>();
        events.add(event);
        store(streamName, expectedVersion, events);
    }
}
