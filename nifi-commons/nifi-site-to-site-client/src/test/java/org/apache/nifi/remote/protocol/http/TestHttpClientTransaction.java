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

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.http.SiteToSiteRestApiUtil;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpInput;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.createDataPacket;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveOneFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveTwoFlowFiles;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveWithInvalidChecksum;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execReceiveZeroFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendButDestinationFull;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendOneFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendTwoFlowFiles;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendWithInvalidChecksum;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.execSendZeroFlowFile;
import static org.apache.nifi.remote.protocol.SiteToSiteTestUtils.readContents;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestHttpClientTransaction {

    private Logger logger = LoggerFactory.getLogger(TestHttpClientTransaction.class);
    private FlowFileCodec codec = new StandardFlowFileCodec();

    private HttpClientTransaction getClientTransaction(InputStream is, OutputStream os, SiteToSiteRestApiUtil apiUtil, TransferDirection direction) throws IOException {
        PeerDescription description = null;
        String peerUrl = "";

        HttpCommunicationsSession commsSession = new HttpCommunicationsSession();
        ((HttpInput)commsSession.getInput()).setInputStream(is);
        ((HttpOutput)commsSession.getOutput()).setOutputStream(os);

        String clusterUrl = "";
        Peer peer = new Peer(description, commsSession, peerUrl, clusterUrl);
        String portId = "portId";
        boolean useCompression = false;
        int penaltyMillis = 1000;
        EventReporter eventReporter = null;
        int protocolVersion = 5;

        HttpClientTransaction transaction = new HttpClientTransaction(protocolVersion, peer, direction, useCompression, portId, penaltyMillis, eventReporter);
        transaction.initialize(apiUtil);

        return transaction;
    }

    @Test
    public void testReceiveZeroFlowFile() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        doReturn(null).when(apiUtil).openConnectionForReceive(eq("portId"), any(CommunicationsSession.class));

        ByteArrayInputStream serverResponse = new ByteArrayInputStream(new byte[0]);
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.RECEIVE);

        execReceiveZeroFlowFile(transaction);

        assertEquals("Client sends nothing as payload to receive flow files.", 0, clientRequest.toByteArray().length);
    }

    @Test
    public void testReceiveOneFlowFile() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tr/transactionId";
        doReturn(holdUri).when(apiUtil).openConnectionForReceive(eq("portId"), any(CommunicationsSession.class));
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
        doReturn(resultEntity).when(apiUtil).commitReceivingFlowFiles(eq(holdUri), eq("3680976076"));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.RECEIVE);

        execReceiveOneFlowFile(transaction);

        assertEquals("Client sends nothing as payload to receive flow files.", 0, clientRequest.toByteArray().length);
        verify(apiUtil).commitReceivingFlowFiles(holdUri, "3680976076");
    }

    @Test
    public void testReceiveTwoFlowFiles() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tr/transactionId";
        doReturn(holdUri).when(apiUtil).openConnectionForReceive(eq("portId"), any(CommunicationsSession.class));
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
        doReturn(resultEntity).when(apiUtil).commitReceivingFlowFiles(eq(holdUri), eq("2969091230"));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        codec.encode(createDataPacket("contents on server 2"), serverResponseBos);
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.RECEIVE);

        execReceiveTwoFlowFiles(transaction);

        assertEquals("Client sends nothing as payload to receive flow files.", 0, clientRequest.toByteArray().length);
        verify(apiUtil).commitReceivingFlowFiles(holdUri, "2969091230");
    }

    @Test
    public void testReceiveWithInvalidChecksum() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tr/transactionId";
        doReturn(holdUri).when(apiUtil).openConnectionForReceive(eq("portId"), any(CommunicationsSession.class));
        // The checksum is correct, but here we simulate as if it's wrong, BAD_CHECKSUM.
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.BAD_CHECKSUM.getCode());
        doReturn(resultEntity).when(apiUtil).commitReceivingFlowFiles(eq(holdUri), eq("2969091230"));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        codec.encode(createDataPacket("contents on server 1"), serverResponseBos);
        codec.encode(createDataPacket("contents on server 2"), serverResponseBos);
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.RECEIVE);

        execReceiveWithInvalidChecksum(transaction);

        assertEquals("Client sends nothing as payload to receive flow files.", 0, clientRequest.toByteArray().length);
        verify(apiUtil).commitReceivingFlowFiles(holdUri, "2969091230");
    }

    @Test
    public void testSendZeroFlowFile() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        doNothing().when(apiUtil).openConnectionForSend(eq("portId"), any(CommunicationsSession.class));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.SEND);

        execSendZeroFlowFile(transaction);

        assertEquals("Client didn't send anything", 0, clientRequest.toByteArray().length);
    }

    @Test
    public void testSendOneFlowFile() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tx/transactionId";
        doNothing().when(apiUtil).openConnectionForSend(eq("portId"), any(CommunicationsSession.class));
        // Emulate that server returns correct checksum.
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                HttpCommunicationsSession commSession = (HttpCommunicationsSession)invocation.getArguments()[0];
                commSession.setChecksum("2946083981");
                return holdUri;
            }
        }).when(apiUtil).finishTransferFlowFiles(any(CommunicationsSession.class));
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED.getCode());
        doReturn(resultEntity).when(apiUtil).commitTransferFlowFiles(eq(holdUri), eq(ResponseCode.CONFIRM_TRANSACTION));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.SEND);

        execSendOneFlowFile(transaction);

        InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiUtil).commitTransferFlowFiles(holdUri, ResponseCode.CONFIRM_TRANSACTION);
    }

    @Test
    public void testSendTwoFlowFiles() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tx/transactionId";
        doNothing().when(apiUtil).openConnectionForSend(eq("portId"), any(CommunicationsSession.class));
        // Emulate that server returns correct checksum.
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                HttpCommunicationsSession commSession = (HttpCommunicationsSession)invocation.getArguments()[0];
                commSession.setChecksum("3359812065");
                return holdUri;
            }
        }).when(apiUtil).finishTransferFlowFiles(any(CommunicationsSession.class));
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED.getCode());
        doReturn(resultEntity).when(apiUtil).commitTransferFlowFiles(eq(holdUri), eq(ResponseCode.CONFIRM_TRANSACTION));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.SEND);

        execSendTwoFlowFiles(transaction);

        InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiUtil).commitTransferFlowFiles(holdUri, ResponseCode.CONFIRM_TRANSACTION);
    }

    @Test
    public void testSendWithInvalidChecksum() throws IOException {
        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tx/transactionId";
        doNothing().when(apiUtil).openConnectionForSend(eq("portId"), any(CommunicationsSession.class));
        // Emulate that server returns incorrect checksum.
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                HttpCommunicationsSession commSession = (HttpCommunicationsSession)invocation.getArguments()[0];
                commSession.setChecksum("Different checksum");
                return holdUri;
            }
        }).when(apiUtil).finishTransferFlowFiles(any(CommunicationsSession.class));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.SEND);

        execSendWithInvalidChecksum(transaction);

        InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiUtil).commitTransferFlowFiles(holdUri, ResponseCode.BAD_CHECKSUM);
    }

    @Test
    public void testSendButDestinationFull() throws IOException {

        SiteToSiteRestApiUtil apiUtil = mock(SiteToSiteRestApiUtil.class);
        final String holdUri = "http://www.example.com/site-to-site/ports/portId/tx/transactionId";
        doNothing().when(apiUtil).openConnectionForSend(eq("portId"), any(CommunicationsSession.class));
        // Emulate that server returns correct checksum.
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                HttpCommunicationsSession commSession = (HttpCommunicationsSession)invocation.getArguments()[0];
                commSession.setChecksum("3359812065");
                return holdUri;
            }
        }).when(apiUtil).finishTransferFlowFiles(any(CommunicationsSession.class));
        TransactionResultEntity resultEntity = new TransactionResultEntity();
        resultEntity.setResponseCode(ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.getCode());
        doReturn(resultEntity).when(apiUtil).commitTransferFlowFiles(eq(holdUri), eq(ResponseCode.CONFIRM_TRANSACTION));

        ByteArrayOutputStream serverResponseBos = new ByteArrayOutputStream();
        ByteArrayInputStream serverResponse = new ByteArrayInputStream(serverResponseBos.toByteArray());
        ByteArrayOutputStream clientRequest = new ByteArrayOutputStream();
        HttpClientTransaction transaction = getClientTransaction(serverResponse, clientRequest, apiUtil, TransferDirection.SEND);

        execSendButDestinationFull(transaction);

        InputStream sentByClient = new ByteArrayInputStream(clientRequest.toByteArray());
        DataPacket packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 1", readContents(packetByClient));
        packetByClient = codec.decode(sentByClient);
        assertEquals("contents on client 2", readContents(packetByClient));
        assertEquals(-1, sentByClient.read());

        verify(apiUtil).commitTransferFlowFiles(holdUri, ResponseCode.CONFIRM_TRANSACTION);
    }
}
