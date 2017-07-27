package uk.co.blackcell.eventsourcing.store.http.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.blackcell.commons.metadata.ThreadLocalMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_CAUSATION_ID_HEADER;
import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_CORRELATION_ID_HEADER;
import static uk.co.blackcell.commons.metadata.FilterConstants.OSL_EVENT_META_PREFIX;


public final class AddEventRequest {

    public final String eventId;
    public final String eventType;
    public final Object data;
    public Map<String, String> metadata;

    private static final Logger LOG = LoggerFactory.getLogger(AddEventRequest.class);


    public AddEventRequest(final UUID eventId, final String eventType, final Object data) {
        this.eventId = eventId.toString();
        this.eventType = eventType;
        this.data = data;

        this.metadata = new HashMap<>(5);

        // Now retrieve ThreadLocalMetadata items that are destined for Event Store metadata

        // Get the special case correlation Id
        String correlationId = ThreadLocalMetadata.get(OSL_CORRELATION_ID_HEADER);
        if( correlationId != null){
            LOG.debug("Adding correlationId {}", correlationId);
            this.metadata.put("$correlationId", correlationId);
        }

        // Form the special case causationId, either from ThreadLocal, if present there, or the eventId
        String causationId = ThreadLocalMetadata.get(OSL_CAUSATION_ID_HEADER);
        if( causationId != null){
            LOG.debug("Adding causationId from thread local {}", causationId);
            this.metadata.put("$causationId", causationId);
        } else {
            LOG.debug("Adding causationId copied from eventId {}", this.eventId);
            this.metadata.put("$causationId", this.eventId);
        }


        // now add anything else that has the special prefix
        Map<String, String> oslExtraMetadata = ThreadLocalMetadata.getStartingWith(OSL_EVENT_META_PREFIX);
        oslExtraMetadata.forEach((key, value) -> {
            LOG.debug("Adding extra metadata {} = {}", key, value);
            this.metadata.put(key, value);
        });

    }
}