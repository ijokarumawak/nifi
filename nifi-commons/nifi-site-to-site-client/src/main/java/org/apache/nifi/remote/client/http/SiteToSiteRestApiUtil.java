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
package org.apache.nifi.remote.client.http;

import org.apache.nifi.remote.codec.HttpFlowFileCodec;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.util.NiFiRestApiUtil;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiteToSiteRestApiUtil extends NiFiRestApiUtil {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteRestApiUtil.class);

    public SiteToSiteRestApiUtil(SSLContext sslContext) {
        super(sslContext);
    }

    public Collection<PeerDTO> getPeers() throws IOException {
        return getEntity("/site-to-site/peers", PeersEntity.class).getPeers();
    }

    public void transferFlowFile(String portId, DataPacket dataPacket) throws IOException {
        logger.info("### Sending transferFlowFile request to port: " + portId);
        HttpURLConnection conn = getConnection("/site-to-site/ports/" + portId);
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-length", String.valueOf(dataPacket.getSize()));
        conn.setRequestProperty("Accept", "application/json");

        Map<String, String> attributes = dataPacket.getAttributes();
        if(attributes != null){
            for(String k : attributes.keySet()){
                conn.setRequestProperty(HttpFlowFileCodec.ATTRIBUTE_HTTP_HEADER_PREFIX + k, attributes.get(k));
            }
        }

        OutputStream outputStream = conn.getOutputStream();
        long transferredByteLen = StreamUtils.copy(dataPacket.getData(), outputStream);
        logger.info("### Sent request to port: " + portId + " transferredByteLen=" + transferredByteLen);

        outputStream.flush();
        outputStream.close();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(conn.getInputStream(), bos);
        String result = bos.toString("UTF-8");
        logger.info("### Sent request to port: " + portId + " result=" + result);
    }

    public DataPacket receiveFlowFile(String portId) throws IOException {
        logger.info("### Sending receiveFlowFile request to port: " + portId);
        HttpURLConnection conn = getConnection("/site-to-site/ports/" + portId);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/octet-stream");

        int responseCode = conn.getResponseCode();
        if(responseCode == 204){
            return null;
        } else if(responseCode != 200){
            // TODO: more sophisticated error handling.
            throw new RuntimeException("Unexpected response code: " + responseCode);
        }

        Map<String, String> attributes = null;
        Map<String, List<String>> responseHeaders = conn.getHeaderFields();
        logger.info("### responseHeaders=" + responseHeaders);
        if(responseHeaders != null){
            attributes = new HashMap<>();
            for(String h : responseHeaders.keySet()){
                // HTTP Status Code is returned as null header key.
                if(h != null && h.startsWith(HttpFlowFileCodec.ATTRIBUTE_HTTP_HEADER_PREFIX)){
                    String k = h.substring(HttpFlowFileCodec.ATTRIBUTE_HTTP_HEADER_PREFIX.length());
                    String v = responseHeaders.get(h).get(0);
                    attributes.put(k, v);
                }
            }
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        StreamUtils.copy(conn.getInputStream(), bos);
        byte[] buf = bos.toByteArray();
        ByteArrayInputStream bin = new ByteArrayInputStream(buf);
        logger.info("### Sent request to port: " + portId + " byteReceived=" + buf.length);
        return new StandardDataPacket(attributes, bin, buf.length);
    }
}
