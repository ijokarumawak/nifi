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

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.HandshakenProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class HttpFlowFileServerProtocol extends AbstractFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "HttpFlowFileProtocol";

    private final FlowFileCodec codec = new StandardFlowFileCodec();
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

    private ConcurrentMap<String, FlowFileTransaction> transactionOnHold;

    public HttpFlowFileServerProtocol(ConcurrentMap<String, FlowFileTransaction> transactionOnHold){
        super();
        this.transactionOnHold = transactionOnHold;
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        return codec;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return codec;
    }

    @Override
    protected HandshakenProperties doHandshake(Peer peer) throws IOException, HandshakeException {
        // TODO: implement handshake logic.
        HandshakenProperties confirmed = new HandshakenProperties();
        logger.debug("{} Done handshake, confirmed={}", this, confirmed);
        return confirmed;
    }

    @Override
    protected void writeTransactionResponse(boolean isTransfer, ResponseCode response, CommunicationsSession commsSession, String explanation) throws IOException {
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) commsSession;

        if(isTransfer){
            switch (response) {
                case NO_MORE_DATA:
                    // TODO: How can I return 204 here?
                    logger.debug("{} There's no data to send.", this);
                    break;
                case CONTINUE_TRANSACTION:
                    logger.debug("{} Continue transaction... expecting more flow files.", this);
                    commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
                    break;
                case BAD_CHECKSUM:
                    logger.debug("{} Received BAD_CHECKSUM.", this);
                    commSession.setStatus(Transaction.TransactionState.ERROR);
                    throw new IOException("Checksum didn't match. BAD_CHECKSUM.");
                case CONFIRM_TRANSACTION:
                    logger.debug("{} Transaction is confirmed.", this);
                    commSession.setStatus(Transaction.TransactionState.TRANSACTION_CONFIRMED);
                    break;
                case FINISH_TRANSACTION:
                    logger.debug("{} transaction is completed.", this);
                    commSession.setStatus(Transaction.TransactionState.TRANSACTION_COMPLETED);
                    break;
            }
        } else {
            switch (response) {
                case CONFIRM_TRANSACTION:
                    logger.debug("{} Confirming transaction. checksum={}", this, explanation);
                    commSession.setChecksum(explanation);
                    commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
                    break;
            }
        }
    }

    @Override
    protected Response readTransactionResponse(boolean isTransfer, CommunicationsSession commsSession) throws IOException {
        // Returns Response based on current status.
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) commsSession;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if(isTransfer){
            switch (commSession.getStatus()){
                case DATA_EXCHANGED:
                    String clientChecksum = ((HttpCommunicationsSession)commsSession).getChecksum();
                    logger.debug("readTransactionResponse. clientChecksum={}", clientChecksum);
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), clientChecksum);
                    break;
                case TRANSACTION_CONFIRMED:
                    logger.debug("readTransactionResponse. finishing.");
                    ResponseCode.TRANSACTION_FINISHED.writeResponse(new DataOutputStream(bos));
                    break;
            }
        } else {
            switch (commSession.getStatus()){
                case TRANSACTION_STARTED:
                    logger.debug("readTransactionResponse. returning CONTINUE_TRANSACTION.");
                    // We don't know if there's more data to receive, so just continue it.
                    ResponseCode.CONTINUE_TRANSACTION.writeResponse(new DataOutputStream(bos));
                    break;
                case TRANSACTION_CONFIRMED:
                    logger.debug("readTransactionResponse. returning CONFIRM_TRANSACTION.");
                    // Checksum was successfully validated at client side.
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(new DataOutputStream(bos), "");
                    break;
            }
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return Response.read(new DataInputStream(bis));
    }

    @Override
    protected int commitTransferTransaction(Peer peer, FlowFileTransaction transaction) throws IOException {
        return holdTransaction(peer, transaction);
    }

    private int holdTransaction(Peer peer, FlowFileTransaction transaction) {
        // We don't commit the session here yet,
        // to avoid losing sent flow files in case some issue happens at client side while it is processing,
        // hold the transaction until we confirm additional request from client.
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        logger.debug("{} Holding transaction. transactionId={}", this, transactionId);
        transactionOnHold.put(transactionId, transaction);

        return transaction.getFlowFilesSent().size();
    }

    public int commitTransferTransaction(Peer peer, String clientChecksum) throws IOException {
        logger.info("{} Committing the transfer transaction. peer={} clientChecksum={}", this, peer, clientChecksum);
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        FlowFileTransaction transaction = transactionOnHold.remove(transactionId);
        if(transaction == null){
            throw new IOException("Transaction was not found. transactionId=" + transactionId);
        }
        commSession.setChecksum(clientChecksum);
        commSession.setStatus(Transaction.TransactionState.DATA_EXCHANGED);
        return super.commitTransferTransaction(peer, transaction);
    }

    @Override
    protected int commitReceiveTransaction(Peer peer, FlowFileTransaction transaction) throws IOException {
        return holdTransaction(peer, transaction);
    }

    public int commitReceiveTransaction(Peer peer) throws IOException {
        logger.info("{} Committing the receive transaction. peer={}", this, peer);
        HttpCommunicationsSession commSession = (HttpCommunicationsSession) peer.getCommunicationsSession();
        String transactionId = commSession.getTransactionId();
        FlowFileTransaction transaction = transactionOnHold.remove(transactionId);
        if(transaction == null){
            throw new IOException("Transaction was not found. transactionId=" + transactionId);
        }
        // TODO: Need to reflect the result sent from client, confirmed, cancel or error.
        commSession.setStatus(Transaction.TransactionState.TRANSACTION_CONFIRMED);
        return super.commitReceiveTransaction(peer, transaction);
    }

    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        return null;
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }

}
