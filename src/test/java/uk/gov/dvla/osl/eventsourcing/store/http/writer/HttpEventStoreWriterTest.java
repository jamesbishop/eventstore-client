package uk.co.blackcell.eventsourcing.store.http.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.lang.RandomStringUtils;
import uk.co.blackcell.commons.metadata.ThreadLocalMetadata;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.configuration.EventStoreConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.blackcell.eventsourcing.impl.SomeEvent;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_CAUSATION_ID_HEADER;
import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_CORRELATION_ID_HEADER;
import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_EVENT_META_PREFIX;

public class HttpEventStoreWriterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpEventStoreWriterTest.class);

    private final String STREAM_NAME = "testStream";
    private HttpEventStoreWriter writer;
    private EventStoreConfiguration configuration;

    private static final String SCHEME = "http";
    private static final String HOST = "localhost";
    private static final Integer PORT = 2115;
    private static final String USERNAME = "testUser";
    private static final String PASSWORD = "pass";
    private static final String PATH = "/streams/.+";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    @Before
    public void setUp() throws IOException {
        configuration = new EventStoreConfiguration(SCHEME, HOST, PORT, USERNAME, PASSWORD);
        writer = new HttpEventStoreWriter(configuration, new ObjectMapper());
        ThreadLocalMetadata.clear();
    }

    @Test
    public void testWriteEventSuccessfully() throws IOException {

        if (!wireMockRule.isRunning()) {
            wireMockRule.start();
        }

        UUID id = UUID.randomUUID();
        LOGGER.info("ID" + id);

        stubFor(post(urlMatching("/streams/testStream"))
                .willReturn(aResponse().withStatus(201)));

        java.util.List<Event> events = new ArrayList<>();

        events.add(new SomeEvent(id, "forename1", "surname1", "email1", new Date(), LocalDate.now()));

        writer.store(STREAM_NAME, 0, events);

        verify(postRequestedFor(urlEqualTo("/streams/testStream")));
    }

    @Test
    public void testWriteEventSuccessfullyWithCorrelationIdMetadata() throws IOException {

        if (!wireMockRule.isRunning()) {
            wireMockRule.start();
        }

        UUID id = UUID.randomUUID();

        stubFor(post(urlMatching("/streams/testStream"))
                .willReturn(aResponse().withStatus(201)));

        java.util.List<Event> events = new ArrayList<>();

        String someUUID = UUID.randomUUID().toString();
        ThreadLocalMetadata.set(OSL_CORRELATION_ID_HEADER, someUUID) ;

        events.add(new SomeEvent(id, "forename1", "surname1", "email1", new Date(), LocalDate.now()));

        writer.store(STREAM_NAME, 0, events);

        verify(postRequestedFor(urlEqualTo("/streams/testStream")).withRequestBody(containing("$correlationId")));
    }

    @Test
    public void testWriteEventSuccessfullyWithCausationIdExtractedFromLocalMetadata() throws IOException {

        if (!wireMockRule.isRunning()) {
            wireMockRule.start();
        }

        UUID id = UUID.randomUUID();
        LOGGER.info("ID: " + id);

        stubFor(post(urlMatching("/streams/testStream"))
                .willReturn(aResponse().withStatus(201)));

        java.util.List<Event> events = new ArrayList<>();

        String someCausation = RandomStringUtils.randomAlphanumeric(17).toUpperCase();
        ThreadLocalMetadata.set(OSL_CAUSATION_ID_HEADER, someCausation) ;

        events.add(new SomeEvent(id, "forename1", "surname1", "email1", new Date(), LocalDate.now()));

        writer.store(STREAM_NAME, 0, events);

        String expectedKV = "$causationId\":\"" + someCausation;

        verify(postRequestedFor(urlEqualTo("/streams/testStream"))
                .withRequestBody(containing(expectedKV))
        );
    }

    @Test
    public void testWriteEventSuccessfullyWithCausationIdDefaultingToEventId() throws IOException {

        if (!wireMockRule.isRunning()) {
            wireMockRule.start();
        }

        UUID driverId = UUID.randomUUID();

        stubFor(post(urlMatching("/streams/testStream"))
                .willReturn(aResponse().withStatus(201)));

        java.util.List<Event> events = new ArrayList<>();

        events.add(new SomeEvent(driverId, "forename1", "surname1", "email1", new Date(), LocalDate.now()));

        writer.store(STREAM_NAME, 0, events);

        // TODO : fix this test to validate that the metadata.$causationId matches the event's eventId.
        // The issue is that the eventId is generated internally, so we need to fish it AND the causationID out
        // of the request at the same time.
        // Needs a bit of Wiremock fu...
        verify(postRequestedFor(urlEqualTo("/streams/testStream"))
                .withRequestBody(containing("$causationId"))
        );
    }


    @Test
    public void testWriteEventSuccessfullyWithExtraMetadata() throws IOException {

        if (!wireMockRule.isRunning()) {
            wireMockRule.start();
        }

        UUID id = UUID.randomUUID();

        stubFor(post(urlMatching("/streams/testStream"))
                .willReturn(aResponse().withStatus(201)));

        java.util.List<Event> events = new ArrayList<>();

        String someMetaKey = OSL_EVENT_META_PREFIX + "someMeta";
        String someMetaValue = RandomStringUtils.randomAlphanumeric(17).toUpperCase();
        ThreadLocalMetadata.set(someMetaKey, someMetaValue) ;

        events.add(new SomeEvent(id, "forename1", "surname1", "email1", new Date(), LocalDate.now()));

        writer.store(STREAM_NAME, 0, events);

        verify(postRequestedFor(urlEqualTo("/streams/testStream"))
                .withRequestBody(containing(someMetaKey))
                .withRequestBody(containing(someMetaValue)));
    }


}
