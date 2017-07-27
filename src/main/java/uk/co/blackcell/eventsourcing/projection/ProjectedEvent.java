package uk.co.blackcell.eventsourcing.projection;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import uk.co.blackcell.eventsourcing.api.Event;

@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class ProjectedEvent {

    private Event event;
    private Integer positionEventNumber;

}
