package uk.co.blackcell.eventsourcing.api;

import uk.co.blackcell.eventsourcing.store.http.entity.Link;

import java.util.List;

public interface LinkProcessor {
    String getUriByRelation(final List<Link> links, final String relationName);
}
