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
package org.apache.nifi.remote.protocol.http;

public class HttpHeaders {

    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "transaction-url";

    public static final String ACCEPT_ENCODING = "Accept-Encoding";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String PROTOCOL_VERSION = "x-nifi-site-to-site-protocol-version";
    public static final String SERVER_SIDE_TRANSACTION_TTL = "x-nifi-site-to-site-server-transaction-ttl";
    /**
     * <p>If this header is returned by the remote NiFi cluster, then it means that
     * a reverse proxy resides in between, and it can route requests to specific node
     * within a remote NiFi cluster using routing header.</p>
     * <p>In such case, Site-to-Site client is encouraged to keep using the bootstrap URL for further communication,
     * instead of sending request to each node directly.</p>
     * @see HttpHeaders#TARGET_PEER
     * @apiNote Since S2S HTTP Protocol Version 2.
     */
    public static final String ROUTE_BY_HEADER = "x-nifi-site-to-site-route-by-header";
    /**
     * <p>Site-to-Site clients can specify the target peer (address:port) with this header.</p>
     * @see HttpHeaders#ROUTE_BY_HEADER
     * @apiNote Since S2S HTTP Protocol Version 2.
     */
    public static final String TARGET_PEER = "x-nifi-site-to-site-target-peer";

    public static final String HANDSHAKE_PROPERTY_USE_COMPRESSION = "x-nifi-site-to-site-use-compression";
    public static final String HANDSHAKE_PROPERTY_REQUEST_EXPIRATION = "x-nifi-site-to-site-request-expiration";
    public static final String HANDSHAKE_PROPERTY_BATCH_COUNT = "x-nifi-site-to-site-batch-count";
    public static final String HANDSHAKE_PROPERTY_BATCH_SIZE = "x-nifi-site-to-site-batch-size";
    public static final String HANDSHAKE_PROPERTY_BATCH_DURATION = "x-nifi-site-to-site-batch-duration";

}
