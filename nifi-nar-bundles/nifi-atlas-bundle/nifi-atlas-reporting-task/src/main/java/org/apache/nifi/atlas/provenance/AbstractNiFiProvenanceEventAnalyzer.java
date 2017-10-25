package org.apache.nifi.atlas.provenance;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.resolver.ClusterResolvers;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;

public abstract class AbstractNiFiProvenanceEventAnalyzer implements NiFiProvenanceEventAnalyzer {

    /**
     * Utility method to parse a string uri silently.
     * @param uri uri to parse
     * @return parsed URI instance
     */
    protected URI parseUri(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            final String msg = String.format("Failed to parse uri %s due to %s", uri, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    protected String toQualifiedName(String clusterName, String dataSetName) {
        return dataSetName + "@" + clusterName;
    }

    protected DataSetRefs singleDataSetRef(String componentId, ProvenanceEventType eventType, Referenceable ref) {
        final DataSetRefs refs = new DataSetRefs(componentId);
        switch (eventType) {
            case SEND:
            case REMOTE_INVOCATION:
                refs.addOutput(ref);
                break;
            case FETCH:
            case RECEIVE:
                refs.addInput(ref);
                break;
        }

        return refs;
    }

    /**
     * Utility method to split comma separated host names. Port number will be removed.
     */
    protected String[] splitHostNames(String hostNames) {
        return Arrays.stream(hostNames.split(","))
                .map(hostName -> hostName.split(":")[0].trim())
                .toArray(String[]::new);
    }

}
