/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.remote;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class PeerDescriptionModifier {

    private static final Logger logger = LoggerFactory.getLogger(PeerDescriptionModifier.class);

    public enum RequestType {
        SiteToSiteDetail,
        Peers
    }

    private static class Route {
        private String name;
        private SiteToSiteTransportProtocol protocol;
        private PreparedQuery predicate;
        private PreparedQuery hostname;
        private PreparedQuery port;
        private PreparedQuery secure;

        private boolean isValid() {
            if (hostname == null) {
                logger.warn("Ignore invalid route definition {} because 'hostname' is not specified.", name);return false;
            }
            if (port == null) {
                logger.warn("Ignore invalid route definition {} because 'port' is not specified.", name);
                return false;
            }
            return true;
        }

        private PeerDescription getTarget(final Map<String, String> variables) {
            final String targetHostName = hostname.evaluateExpressions(variables, null);
            if (isBlank(targetHostName)) {
                throw new IllegalStateException("Target hostname was not resolved for the route definition " + name);
            }

            final String targetPortStr = port.evaluateExpressions(variables, null);
            if (isBlank(targetPortStr)) {
                throw new IllegalStateException("Target port was not resolved for the route definition " + name);
            }

            String targetIsSecure = secure.evaluateExpressions(variables, null);
            if (isBlank(targetIsSecure)) {
                targetIsSecure = "false";
            }
            return new PeerDescription(targetHostName, Integer.valueOf(targetPortStr), Boolean.valueOf(targetIsSecure));
        }
    }

    private Map<SiteToSiteTransportProtocol, List<Route>> routes;


    private static final String PROPERTY_PREFIX = "nifi.remote.route.";

    public PeerDescriptionModifier(final NiFiProperties properties) {
        final Map<String, List<String>> routeDefinitions = properties.getPropertyKeys().stream()
                .filter(k -> k.startsWith(PROPERTY_PREFIX))
                .collect(Collectors.groupingBy(k -> k.substring(PROPERTY_PREFIX.length(), k.lastIndexOf('.'))));

        routes = routeDefinitions.entrySet().stream().map(r -> {
            final Route route = new Route();
            final String[] key = r.getKey().split("\\.");
            route.protocol = SiteToSiteTransportProtocol.valueOf(key[0].toUpperCase());
            route.name = key[1];
            r.getValue().forEach(k -> {
                final String name = k.substring(k.lastIndexOf('.') + 1);
                final String value = properties.getProperty(k);
                switch (name) {
                    case "when":
                        route.predicate = Query.prepare(value);
                        break;
                    case "hostname":
                        route.hostname = Query.prepare(value);
                        break;
                    case "port":
                        route.port = Query.prepare(value);
                        break;
                    case "secure":
                        route.secure = Query.prepare(value);
                        break;
                }
            });
            return route;
        }).filter(Route::isValid).collect(Collectors.groupingBy(r -> r.protocol));

    }

    private void addVariables(Map<String, String> map, String prefix, PeerDescription peer) {
        map.put(format("%s.hostname", prefix), peer.getHostname());
        map.put(format("%s.port", prefix), String.valueOf(peer.getPort()));
        map.put(format("%s.secure", prefix), String.valueOf(peer.isSecure()));
    }

    public boolean isModificationNeeded(final SiteToSiteTransportProtocol protocol) {
        return routes != null && routes.containsKey(protocol) && !routes.get(protocol).isEmpty();
    }

    /**
     * Modifies target peer description so that subsequent request can go through the appropriate route
     * @param source The source peer from which a request was sent, this can be any server host participated to relay the request,
     *              but should be the one which can contribute to derive the correct target peer.
     * @param target The original target which should receive and process further incoming requests.
     * @param protocol The S2S protocol being used.
     * @param requestType The requested API type.
     * @param variables Containing context variables those can be referred from Expression Language.
     * @return A peer description. The original target peer can be returned if there is no intermediate peer such as reverse proxies needed.
     */
    public PeerDescription modify(final PeerDescription source, final PeerDescription target,
                                  final SiteToSiteTransportProtocol protocol, final RequestType requestType,
                                  final Map<String, String> variables) {

        addVariables(variables, "s2s.source", source);
        addVariables(variables, "s2s.target", target);
        variables.put("s2s.protocol", protocol.name());
        variables.put("s2s.request", requestType.name());

        logger.debug("Modifying PeerDescription, variables={}", variables);

        return routes.get(protocol).stream().filter(r -> r.predicate == null
                || Boolean.valueOf(r.predicate.evaluateExpressions(variables, null)))
                .map(r -> {
                    final PeerDescription t = r.getTarget(variables);
                    logger.debug("Route definition {} matched, {}", r.name, t);
                    return t;
                })
                // If a matched route was found, use it, else use the original target.
                .findFirst().orElse(target);

    }
}
