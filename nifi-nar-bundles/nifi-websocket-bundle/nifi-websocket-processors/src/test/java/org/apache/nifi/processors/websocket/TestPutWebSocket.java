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
package org.apache.nifi.processors.websocket;

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_BROADCAST_FAILED;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_BROADCAST_SUCCEEDED;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_FAILURE_DETAIL;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.websocket.AbstractWebSocketSession;
import org.apache.nifi.websocket.SendMessage;
import org.apache.nifi.websocket.SessionNotFoundException;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketService;
import org.apache.nifi.websocket.WebSocketSession;
import org.junit.Test;


public class TestPutWebSocket {

    private void assertFlowFile(WebSocketSession webSocketSession, String serviceId, String endpointId, MockFlowFile ff, WebSocketMessage.Type messageType) {
        assertEquals(serviceId, ff.getAttribute(ATTR_WS_CS_ID));
        assertEquals(webSocketSession.getSessionId(), ff.getAttribute(ATTR_WS_SESSION_ID));
        assertEquals(endpointId, ff.getAttribute(ATTR_WS_ENDPOINT_ID));
        assertEquals(webSocketSession.getLocalAddress().toString(), ff.getAttribute(ATTR_WS_LOCAL_ADDRESS));
        assertEquals(webSocketSession.getRemoteAddress().toString(), ff.getAttribute(ATTR_WS_REMOTE_ADDRESS));
        assertEquals(messageType != null ? messageType.name() : null, ff.getAttribute(ATTR_WS_MESSAGE_TYPE));
    }

    private WebSocketSession getWebSocketSession(String sessionId) {
        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn(sessionId);
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));
        when(webSocketSession.getTransitUri()).thenReturn("ws://example.com/web-socket");
        return webSocketSession;
    }

    private WebSocketSession getWebSocketSession() {
        return getWebSocketSession("ws-session-id");
    }

    @Test
    public void testSessionIsNotSpecified() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add(webSocketSession.getSessionId());
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        //assertEquals(0, succeededFlowFiles.size());   //No longer valid test after NIFI-3318 since not specifying sessionid will send to all clients
        assertEquals(1, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        //assertEquals(1, failedFlowFiles.size());      //No longer valid test after NIFI-3318
        assertEquals(0, failedFlowFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());

    }

    @Test
    public void testServiceIsNotFound() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final ControllerService service = spy(ControllerService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, "different-service-id");
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        final MockFlowFile failedFlowFile = failedFlowFiles.iterator().next();
        assertNotNull(failedFlowFile.getAttribute(ATTR_WS_FAILURE_DETAIL));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());

    }

    @Test
    public void testServiceIsNotWebSocketService() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final ControllerService service = spy(ControllerService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        final MockFlowFile failedFlowFile = failedFlowFiles.iterator().next();
        assertNotNull(failedFlowFile.getAttribute(ATTR_WS_FAILURE_DETAIL));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());

    }

    @Test
    public void testNoConnectedSessions() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        runner.setProperty(PutWebSocket.PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS, "true");
        final WebSocketService service = spy(WebSocketService.class);

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_NO_CONNECTED_SESSIONS);
        assertEquals(1, failedFlowFiles.size());
        final MockFlowFile failedFlowFile = failedFlowFiles.iterator().next();
        assertEquals(PutWebSocket.NO_CONNECTED_WEB_SOCKET_SESSIONS, failedFlowFile.getAttribute(ATTR_WS_FAILURE_DETAIL));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());
    }

    @Test
    public void testSendFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add(webSocketSession.getSessionId());
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        doThrow(new IOException("Sending message failed.")).when(webSocketSession).sendString(anyString());
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        final MockFlowFile failedFlowFile = failedFlowFiles.iterator().next();
        assertNotNull(failedFlowFile.getAttribute(ATTR_WS_FAILURE_DETAIL));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());

    }

    @Test
    public void testSendFailureDetailedRelationship() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        runner.setProperty(PutWebSocket.PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS, "true");
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add(webSocketSession.getSessionId());
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        doThrow(new IOException("Sending message failed.")).when(webSocketSession).sendString(anyString());
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_COMMUNICATION_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        final MockFlowFile failedFlowFile = failedFlowFiles.iterator().next();
        assertNotNull(failedFlowFile.getAttribute(ATTR_WS_FAILURE_DETAIL));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());

    }

    @Test
    public void testSuccess() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add(webSocketSession.getSessionId());
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(PutWebSocket.PROP_WS_MESSAGE_TYPE, "${" + ATTR_WS_MESSAGE_TYPE + "}");

        // Enqueue 1st file as Text.
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        // Enqueue 2nd file as Binary.
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.BINARY.name());
        runner.enqueue(textMessageFromServer.getBytes(), attributes);

        runner.run(2);

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(2, succeededFlowFiles.size());
        assertFlowFile(webSocketSession, serviceId, endpointId, succeededFlowFiles.get(0), WebSocketMessage.Type.TEXT);
        assertFlowFile(webSocketSession, serviceId, endpointId, succeededFlowFiles.get(1), WebSocketMessage.Type.BINARY);

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(0, failedFlowFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(2, provenanceEvents.size());
    }

    @Test
    public void testSessionUnknown() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(new HashSet<>());
        doThrow(new SessionNotFoundException("Not found")).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        attributes.put(ATTR_WS_SESSION_ID, webSocketSession.getSessionId());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(1, failedFlowFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());
    }

    @Test
    public void testBroadcast_FailureAll() throws Exception {
        // test failed sending to all broadcast client
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        runner.setProperty(PutWebSocket.PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS, "true");
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession1 = getWebSocketSession("ws-session-id-1");
        final WebSocketSession webSocketSession3 = getWebSocketSession("ws-session-id-3");

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add("ws-session-id-1");
        sessionIds.add("ws-session-id-2");
        sessionIds.add("ws-session-id-3");
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);

        // For session 1
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession1);
            return null;
        }).when(service).sendMessage(anyString(), eq("ws-session-id-1"), any(SendMessage.class));
        doThrow(new IOException("Sending message failed."))
                .when(webSocketSession1).sendString(anyString());

        // For session 2
        doThrow(new SessionNotFoundException("Simulate the second session is removed"))
                .when(service).sendMessage(anyString(), eq("ws-session-id-2"), any(SendMessage.class));

        // For session 3
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession3);
            return null;
        }).when(service).sendMessage(anyString(), eq("ws-session-id-3"), any(SendMessage.class));
        doThrow(new IOException("Sending message failed."))
                .when(webSocketSession3).sendString(anyString());


        runner.addControllerService(serviceId, service);
        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        // The original FlowFile should be transferred when configured not to fork.
        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_COMMUNICATION_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        MockFlowFile failedFlowFile = failedFlowFiles.get(0);
        failedFlowFile.assertAttributeNotExists(ATTR_WS_SESSION_ID);
        failedFlowFile.assertAttributeEquals(ATTR_WS_BROADCAST_SUCCEEDED, "0");
        failedFlowFile.assertAttributeEquals(ATTR_WS_BROADCAST_FAILED, "3");


        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(0, provenanceEvents.size());
    }

    @Test
    public void testBroadcast_FailureAll_Fork() throws Exception {
        // test failed sending to all broadcast client
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        runner.setProperty(PutWebSocket.PROP_ENABLE_DETAILED_FAILURE_RELATIONSHIPS, "true");
        runner.setProperty(PutWebSocket.PROP_FORK_FAILED_BROADCAST_SESSIONS, "true");
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession1 = getWebSocketSession("ws-session-id-1");
        final WebSocketSession webSocketSession3 = getWebSocketSession("ws-session-id-3");

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final Set<String> sessionIds = new LinkedHashSet<>();
        sessionIds.add("ws-session-id-1");
        sessionIds.add("ws-session-id-2");
        sessionIds.add("ws-session-id-3");
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);

        // For session 1
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession1);
            return null;
        }).when(service).sendMessage(anyString(), eq("ws-session-id-1"), any(SendMessage.class));
        doThrow(new IOException("Sending message failed."))
                .when(webSocketSession1).sendString(anyString());

        // For session 2
        doThrow(new SessionNotFoundException("Simulate the second session is removed"))
                .when(service).sendMessage(anyString(), eq("ws-session-id-2"), any(SendMessage.class));

        // For session 3
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession3);
            return null;
        }).when(service).sendMessage(anyString(), eq("ws-session-id-3"), any(SendMessage.class));
        doThrow(new IOException("Sending message failed."))
                .when(webSocketSession3).sendString(anyString());


        runner.addControllerService(serviceId, service);
        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(0, succeededFlowFiles.size());

        final List<MockFlowFile> communicationErrors = runner.getFlowFilesForRelationship(PutWebSocket.REL_COMMUNICATION_FAILURE);
        assertEquals(2, communicationErrors.size());

        MockFlowFile commError1 = communicationErrors.get(0);
        commError1.assertAttributeEquals(ATTR_WS_SESSION_ID, "ws-session-id-1");
        commError1.assertAttributeEquals(ATTR_WS_BROADCAST_SUCCEEDED, "0");
        commError1.assertAttributeEquals(ATTR_WS_BROADCAST_FAILED, "3");

        MockFlowFile commError2 = communicationErrors.get(1);
        commError2.assertAttributeEquals(ATTR_WS_SESSION_ID, "ws-session-id-3");
        commError2.assertAttributeEquals(ATTR_WS_BROADCAST_SUCCEEDED, "0");
        commError2.assertAttributeEquals(ATTR_WS_BROADCAST_FAILED, "3");

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(1, failedFlowFiles.size());
        MockFlowFile failed = failedFlowFiles.get(0);
        failed.assertAttributeEquals(ATTR_WS_SESSION_ID, "ws-session-id-2");
        failed.assertAttributeEquals(ATTR_WS_BROADCAST_SUCCEEDED, "0");
        failed.assertAttributeEquals(ATTR_WS_BROADCAST_FAILED, "3");

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(3, provenanceEvents.size());                                       // logging the FORK
        assertEquals(provenanceEvents.get(0).getEventType(), ProvenanceEventType.FORK); // verify EventType
    }

    @Test
    public void testBroadcast_SuccessAll() throws Exception {
        // test success sending to all broadcast clients
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add("ws-session-id-1");
        sessionIds.add("ws-session-id-2");
        sessionIds.add("ws-session-id-3");
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));

        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(PutWebSocket.PROP_WS_MESSAGE_TYPE, "${" + ATTR_WS_MESSAGE_TYPE + "}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(1, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(0, failedFlowFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        assertEquals(provenanceEvents.get(0).getEventType(), ProvenanceEventType.SEND); // verify EventType
    }

    @Test
    public void testBroadcast_1Success_2Failure() throws Exception {
        // test success and failure sending to some broadcast clients
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add("ws-session-id-1");
        sessionIds.add("ws-session-id-2");
        sessionIds.add("ws-session-id-3");
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);

        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            throw new IOException("Sending message failed.");
        }).when(service).sendMessage(anyString(), eq("ws-session-id-2"), any(SendMessage.class));
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            throw new IOException("Sending message failed.");
        }).when(service).sendMessage(anyString(), eq("ws-session-id-3"), any(SendMessage.class));

        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(1, succeededFlowFiles.size());

        runner.assertTransferCount(PutWebSocket.REL_FAILURE, 0);
        runner.assertTransferCount(PutWebSocket.REL_COMMUNICATION_FAILURE, 0);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());                       // logging SEND
    }

    @Test
    public void testBroadcast_1Success_2Failure_Fork() throws Exception {
        // test success and failure sending to some broadcast clients
        final TestRunner runner = TestRunners.newTestRunner(PutWebSocket.class);
        runner.setProperty(PutWebSocket.PROP_FORK_FAILED_BROADCAST_SESSIONS, "true");
        final WebSocketService service = spy(WebSocketService.class);

        final WebSocketSession webSocketSession = getWebSocketSession();

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        final HashSet<String> sessionIds = new HashSet<>();
        sessionIds.add("ws-session-id-1");
        sessionIds.add("ws-session-id-2");
        sessionIds.add("ws-session-id-3");
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getSessionIds(endpointId)).thenReturn(sessionIds);

        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            return null;
        }).when(service).sendMessage(anyString(), anyString(), any(SendMessage.class));
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            throw new IOException("Sending message failed.");
        }).when(service).sendMessage(anyString(), eq("ws-session-id-2"), any(SendMessage.class));
        doAnswer(invocation -> {
            final SendMessage sendMessage = invocation.getArgumentAt(2, SendMessage.class);
            sendMessage.send(webSocketSession);
            throw new IOException("Sending message failed.");
        }).when(service).sendMessage(anyString(), eq("ws-session-id-3"), any(SendMessage.class));

        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_WS_CS_ID, serviceId);
        attributes.put(ATTR_WS_ENDPOINT_ID, endpointId);
        attributes.put(ATTR_WS_MESSAGE_TYPE, WebSocketMessage.Type.TEXT.name());
        runner.enqueue(textMessageFromServer, attributes);

        runner.run();

        final List<MockFlowFile> succeededFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_SUCCESS);
        assertEquals(1, succeededFlowFiles.size());

        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutWebSocket.REL_FAILURE);
        assertEquals(2, failedFlowFiles.size());

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(3, provenanceEvents.size());                       // logging SEND and FORK
        int sendCnt = 0, forkCnt = 0;
        for (ProvenanceEventRecord provEvent : provenanceEvents ) {
            if (provEvent.getEventType() == ProvenanceEventType.SEND)
                sendCnt ++;
            else if (provEvent.getEventType() == ProvenanceEventType.FORK)
                forkCnt ++;
        }
        assertEquals(1, sendCnt); // verify EventType SEND (due to success)
        assertEquals(2, forkCnt); // verify EventType FORK (due to failure)
    }

}
