package uk.co.blackcell.eventsourcing.store.http.reader;

import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;
import uk.co.blackcell.eventsourcing.api.DataFetcher;
import uk.co.blackcell.eventsourcing.configuration.EventStoreConfiguration;
import uk.co.blackcell.eventsourcing.exception.EventStoreClientTechnicalException;
import uk.co.blackcell.eventsourcing.store.http.EventStoreService;
import uk.co.blackcell.eventsourcing.store.http.entity.EventStreamData;
import uk.co.blackcell.eventsourcing.store.http.entity.HealthCheck;

import java.io.IOException;

public class StreamDataFetcher implements DataFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpEventStoreReader.class);
    private final EventStoreService service;
    private EventStoreConfiguration configuration;

    public StreamDataFetcher(final EventStoreService service,
                             EventStoreConfiguration configuration) {
        this.service = service;
        this.configuration = configuration;
    }

    @Override
    public EventStreamData fetchStreamData(final String url, final boolean longPoll) throws IOException {

        if (longPoll)
            LOGGER.trace("Starting long-poll with value of " + configuration.getProjectionConfiguration().getLongPollSeconds());

        final Call<EventStreamData> eventStream = service.getEventStreamData(longPoll ? configuration.getProjectionConfiguration().getLongPollSeconds() : null, url + "?embed=body");

        try {
            final Response<EventStreamData> response = eventStream.execute();

            if (response.isSuccessful())
                return response.body();
            else {
                String message;

                final HttpUrl httpUrl = new HttpUrl.Builder()
                        .scheme(configuration.getScheme())
                        .host(configuration.getHost())
                        .port(configuration.getPort())
                        .build();

                if (pingEventStore()) {
                    message = "fetchStreamData failed on event stream: " + httpUrl.toString() + url + ".  " +
                            "The event store is available, but the stream does not exist.  " +
                            "This is probably because no events have been written to this stream. " +
                            response.errorBody();
                } else {
                    message = "fetchStreamData failed on event stream: " + httpUrl.toString() + url + ".  " +
                            "This has occurred because the event store is unreachable, down. " +
                            response.errorBody();

                }
                throw new EventStoreClientTechnicalException(message);
            }
        } catch (Exception e) {
            throw new EventStoreClientTechnicalException(e.getMessage(), e);
        }
    }

    private boolean pingEventStore() {

        try {
            Response<HealthCheck> pingResponse = service.ping().execute();
            return pingResponse.isSuccessful();
        } catch (Exception e) {
            throw new EventStoreClientTechnicalException("The event store is not available", e);
        }
    }
}
