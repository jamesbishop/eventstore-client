package uk.co.blackcell.eventsourcing.impl;

import com.fasterxml.jackson.annotation.JsonFormat;

import uk.co.blackcell.eventsourcing.api.Event;

import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

public class SomeEvent implements Event {
    public final UUID driverId;
    public final String forename;
    public final String surname;
    public final String email;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    public final Date eventDate;

    public final LocalDate regDate;

    SomeEvent() {
        this.driverId = null;
        this.forename = null;
        this.surname = null;
        this.email = null;
        this.eventDate = null;
        this.regDate = null;
    }

    public SomeEvent(UUID driverId, String forename, String surname, String email, Date eventDate, LocalDate regDate) {
        this.driverId = driverId;
        this.forename = forename;
        this.surname = surname;
        this.email = email;
        this.eventDate = eventDate;
        this.regDate = regDate;
    }

    @Override
    public UUID aggregateId() {
        return driverId;
    }
}
