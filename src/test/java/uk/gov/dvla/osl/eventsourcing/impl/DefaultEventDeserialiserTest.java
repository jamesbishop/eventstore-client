package uk.co.blackcell.eventsourcing.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventDeserialiser;
import uk.co.blackcell.eventsourcing.exception.EventDeserialisationException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

public class DefaultEventDeserialiserTest {

    @Test(expected = EventDeserialisationException.class)
    public void invalidDataMustThrowEventDeserialisationException() throws IOException, ClassNotFoundException {

        // Arrange
        //
        String data = "yadayada";
        EventDeserialiser eventDeserialiser = new DefaultEventDeserialiser();

        // Act
        //
        eventDeserialiser.deserialise(data, SomeEvent.class.getName());
    }

    @Test(expected = EventDeserialisationException.class)
    public void invalidEventTypeMustThrowEventDeserialisationException() throws IOException, ClassNotFoundException {

        // Arrange
        //
        String data = "yadayada";
        EventDeserialiser eventDeserialiser = new DefaultEventDeserialiser();

        // Act
        //
        eventDeserialiser.deserialise(data, "uk.co.blackcell.memory.UnknownEvent");
    }

    @Test
    public void validDataAndEventTypeMustResultInValidEvent() throws IOException, ClassNotFoundException {

        // Arrange
        //
        Date eventDate = new Date();
        UUID eventId = UUID.randomUUID();

        LocalDate regDate = LocalDate.now();
        SomeEvent testEvent = new SomeEvent(eventId, "adebayo", "akinfenwa", "emailaddress", eventDate, regDate);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        String data = mapper.writeValueAsString(testEvent);

        // Act
        //
        EventDeserialiser eventDeserialiser = new DefaultEventDeserialiser();
        Event event = eventDeserialiser.deserialise(data, SomeEvent.class.getName());
        SomeEvent someEvent = (SomeEvent)event;

        // Assert
        //
        Assert.assertEquals(eventId, someEvent.driverId);
        Assert.assertEquals("adebayo", someEvent.forename);
        Assert.assertEquals("akinfenwa", someEvent.surname);
        Assert.assertEquals("emailaddress", someEvent.email);
        Assert.assertEquals(eventDate.toString(), someEvent.eventDate.toString());
        Assert.assertEquals(regDate , someEvent.regDate);
    }
}
