package uk.co.blackcell.eventsourcing.impl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import uk.co.blackcell.eventsourcing.api.Event;
import uk.co.blackcell.eventsourcing.api.EventDeserialiser;
import uk.co.blackcell.eventsourcing.exception.EventDeserialisationException;

public class DefaultEventDeserialiser implements EventDeserialiser {

    private ObjectMapper mapper;

    public DefaultEventDeserialiser() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    public Event deserialise(final String data, final String eventType)  {

        Class clazz;
        try {

            clazz = Class.forName(eventType);
            return (Event) mapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new EventDeserialisationException(data, eventType, e);
        }
    }
}
