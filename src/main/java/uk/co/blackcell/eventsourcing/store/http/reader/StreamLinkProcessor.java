package uk.co.blackcell.eventsourcing.store.http.reader;

import uk.co.blackcell.eventsourcing.api.LinkProcessor;
import uk.co.blackcell.eventsourcing.store.http.entity.Link;

import java.util.List;

public class StreamLinkProcessor implements LinkProcessor {

    @Override
    public String getUriByRelation(final List<Link> links, final String relationName) {
        for (Link link : links) {
            if (link.getRelation().equals(relationName))
                return link.getUri();
        }
        return "";
    }
}
