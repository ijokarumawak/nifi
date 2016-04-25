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
package org.apache.nifi.remote.protocol;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.socket.Response;
import org.apache.nifi.remote.protocol.socket.ResponseCode;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public abstract class AbstractFlowFileServerProtocol implements ServerProtocol {

    protected ProcessGroup rootGroup;
    protected RootGroupPort port;

    protected boolean handshakeCompleted;
    protected boolean shutdown = false;
    protected FlowFileCodec negotiatedFlowFileCodec = null;

    protected HandshakenProperties handshakenProperties;

    protected static final long DEFAULT_BATCH_NANOS = TimeUnit.SECONDS.toNanos(5L);

    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void setRootProcessGroup(final ProcessGroup group) {
        if (!group.isRootGroup()) {
            throw new IllegalArgumentException();
        }
        this.rootGroup = group;
    }

    @Override
    public boolean isHandshakeSuccessful() {
        return handshakeCompleted;
    }

    @Override
    public RootGroupPort getPort() {
        return port;
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return negotiatedFlowFileCodec;
    }

    @Override
    public final void handshake(final Peer peer) throws IOException, HandshakeException {
        if (handshakeCompleted) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Handshaking with {}", this, peer);

        this.handshakenProperties = doHandshake(peer);

        logger.debug("{} Finished handshake with {}", this, peer);
        handshakeCompleted = true;
    }

    abstract protected HandshakenProperties doHandshake(final Peer peer) throws  IOException, HandshakeException;

    @Override
    public int transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Sending FlowFiles to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        String remoteDn = commsSession.getUserDn();
        if (remoteDn == null) {
            remoteDn = "none";
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // we have no data to send. Notify the peer.
            logger.debug("{} No data to send to {}", this, peer);
            writeTransactionResponse(ResponseCode.NO_MORE_DATA, commsSession);
            return 0;
        }

        // we have data to send.
        logger.debug("{} Data is available to send to {}", this, peer);
        writeTransactionResponse(ResponseCode.MORE_DATA, commsSession);

        final StopWatch stopWatch = new StopWatch(true);
        long bytesSent = 0L;
        final Set<FlowFile> flowFilesSent = new HashSet<>();
        final CRC32 crc = new CRC32();

        // send data until we reach some batch size
        boolean continueTransaction = true;
        final long startNanos = System.nanoTime();
        String calculatedCRC = "";
        OutputStream os = getFlowFileOutputStream(commsSession);
        while (continueTransaction) {
            final boolean useGzip = handshakenProperties.isUseGzip();
            final OutputStream flowFileOutputStream = useGzip ? new CompressionOutputStream(os) : os;
            logger.debug("{} Sending {} to {}", new Object[]{this, flowFile, peer});

            final CheckedOutputStream checkedOutputStream = new CheckedOutputStream(flowFileOutputStream, crc);

            final StopWatch transferWatch = new StopWatch(true);

            final FlowFile toSend = flowFile;
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    final DataPacket dataPacket = new StandardDataPacket(toSend.getAttributes(), in, toSend.getSize());
                    codec.encode(dataPacket, checkedOutputStream);
                }
            });

            final long transmissionMillis = transferWatch.getElapsed(TimeUnit.MILLISECONDS);

            // need to close the CompressionOutputStream in order to force it write out any remaining bytes.
            // Otherwise, do NOT close it because we don't want to close the underlying stream
            // (CompressionOutputStream will not close the underlying stream when it's closed)
            if (useGzip) {
                checkedOutputStream.close();
            }

            flowFilesSent.add(flowFile);
            bytesSent += flowFile.getSize();

            String transitUriPrefix = handshakenProperties.getTransitUriPrefix();
            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + flowFile.getAttribute(CoreAttributes.UUID.key());
            session.getProvenanceReporter().send(flowFile, transitUri, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transmissionMillis, false);
            session.remove(flowFile);

            // determine if we should check for more data on queue.
            final long sendingNanos = System.nanoTime() - startNanos;
            boolean poll = true;
            double batchDurationNanos = handshakenProperties.getBatchDurationNanos();
            if (sendingNanos >= batchDurationNanos && batchDurationNanos > 0L) {
                poll = false;
            }
            double batchBytes = handshakenProperties.getBatchBytes();
            if (bytesSent >= batchBytes && batchBytes > 0L) {
                poll = false;
            }
            double batchCount = handshakenProperties.getBatchCount();
            if (flowFilesSent.size() >= batchCount && batchCount > 0) {
                poll = false;
            }

            if (batchDurationNanos == 0 && batchBytes == 0 && batchCount == 0) {
                poll = (sendingNanos < DEFAULT_BATCH_NANOS);
            }

            if (poll) {
                // we've not elapsed the requested sending duration, so get more data.
                flowFile = session.get();
            } else {
                flowFile = null;
            }

            continueTransaction = (flowFile != null);
            if (continueTransaction) {
                logger.debug("{} Sending ContinueTransaction indicator to {}", this, peer);
                writeTransactionResponse(ResponseCode.CONTINUE_TRANSACTION, commsSession);
            } else {
                logger.debug("{} Sending FinishTransaction indicator to {}", this, peer);
                writeTransactionResponse(ResponseCode.FINISH_TRANSACTION, commsSession);
                calculatedCRC = String.valueOf(checkedOutputStream.getChecksum().getValue());
            }
        }

        FlowFileTransaction tx = new FlowFileTransaction(session, stopWatch, bytesSent, flowFilesSent, calculatedCRC);
        return commitTransferTransaction(peer, tx);

    }

    protected int commitTransferTransaction(Peer peer, FlowFileTransaction tx) throws IOException {
        ProcessSession session = tx.getSession();
        Set<FlowFile> flowFilesSent = tx.getFlowFilesSent();

        // we've sent a FINISH_TRANSACTION. Now we'll wait for the peer to send a 'Confirm Transaction' response
        CommunicationsSession commsSession = peer.getCommunicationsSession();
        final Response transactionConfirmationResponse = readTransactionResponse(commsSession);
        if (transactionConfirmationResponse.getCode() == ResponseCode.CONFIRM_TRANSACTION) {
            // Confirm Checksum and echo back the confirmation.
            logger.debug("{} Received {}  from {}", this, transactionConfirmationResponse, peer);
            final String receivedCRC = transactionConfirmationResponse.getMessage();

            if (versionNegotiator.getVersion() > 3) {
                String calculatedCRC = tx.getCalculatedCRC();
                if (!receivedCRC.equals(calculatedCRC)) {
                    writeTransactionResponse(ResponseCode.BAD_CHECKSUM, commsSession);
                    session.rollback();
                    throw new IOException(this + " Sent data to peer " + peer + " but calculated CRC32 Checksum as "
                            + calculatedCRC + " while peer calculated CRC32 Checksum as " + receivedCRC
                            + "; canceling transaction and rolling back session");
                }
            }

            writeTransactionResponse(ResponseCode.CONFIRM_TRANSACTION, commsSession);

        } else {
            throw new ProtocolException("Expected to receive 'Confirm Transaction' response from peer " + peer + " but received " + transactionConfirmationResponse);
        }

        final String flowFileDescription = flowFilesSent.size() < 20 ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";

        final Response transactionResponse;
        try {
            transactionResponse = readTransactionResponse(commsSession);
        } catch (final IOException e) {
            logger.error("{} Failed to receive a response from {} when expecting a TransactionFinished Indicator."
                    + " It is unknown whether or not the peer successfully received/processed the data."
                    + " Therefore, {} will be rolled back, possibly resulting in data duplication of {}",
                    this, peer, session, flowFileDescription);
            session.rollback();
            throw e;
        }

        logger.debug("{} received {} from {}", new Object[]{this, transactionResponse, peer});
        if (transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL) {
            peer.penalize(port.getIdentifier(), port.getYieldPeriod(TimeUnit.MILLISECONDS));
        } else if (transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED) {
            throw new ProtocolException("After sending data, expected TRANSACTION_FINISHED response but got " + transactionResponse);
        }

        session.commit();

        StopWatch stopWatch = tx.getStopWatch();
        long bytesSent = tx.getBytesSent();
        stopWatch.stop();
        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesSent);
        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[]{
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesSent.size();
    }

    protected OutputStream getFlowFileOutputStream(CommunicationsSession commsSession) throws IOException {
        return new DataOutputStream(commsSession.getOutput().getOutputStream());
    }

    // TODO: The argument can be Inputstream
    protected Response readTransactionResponse(CommunicationsSession commsSession) throws IOException {
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        return Response.read(dis);
    }

    // TODO: The argument can be OutputStream
    protected void writeTransactionResponse(ResponseCode response, CommunicationsSession commsSession) throws IOException {
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        response.writeResponse(dos);
    }

    @Override
    public int receiveFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} receiving FlowFiles from {}", this, peer);

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String remoteDn = commsSession.getUserDn();
        if (remoteDn == null) {
            remoteDn = "none";
        }

        final StopWatch stopWatch = new StopWatch(true);
        final CRC32 crc = new CRC32();

        // Peer has data. Otherwise, we would not have been called, because they would not have sent
        // a SEND_FLOWFILES request to use. Just decode the bytes into FlowFiles until peer says he's
        // finished sending data.
        final Set<FlowFile> flowFilesReceived = new HashSet<>();
        long bytesReceived = 0L;
        boolean continueTransaction = true;
        String calculatedCRC = "";
        while (continueTransaction) {
            final long startNanos = System.nanoTime();
            final InputStream flowFileInputStream = handshakenProperties.isUseGzip() ? new CompressionInputStream(dis) : dis;
            final CheckedInputStream checkedInputStream = new CheckedInputStream(flowFileInputStream, crc);

            final DataPacket dataPacket = codec.decode(checkedInputStream);
            FlowFile flowFile = session.create();
            flowFile = session.importFrom(dataPacket.getData(), flowFile);
            flowFile = session.putAllAttributes(flowFile, dataPacket.getAttributes());

            final long transferNanos = System.nanoTime() - startNanos;
            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
            final String sourceSystemFlowFileUuid = dataPacket.getAttributes().get(CoreAttributes.UUID.key());
            flowFile = session.putAttribute(flowFile, CoreAttributes.UUID.key(), UUID.randomUUID().toString());

            String transitUriPrefix = handshakenProperties.getTransitUriPrefix();
            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceSystemFlowFileUuid;
            session.getProvenanceReporter().receive(flowFile, transitUri, sourceSystemFlowFileUuid == null
                    ? null : "urn:nifi:" + sourceSystemFlowFileUuid, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transferMillis);
            session.transfer(flowFile, Relationship.ANONYMOUS);
            flowFilesReceived.add(flowFile);
            bytesReceived += flowFile.getSize();

            final Response transactionResponse = Response.read(dis);
            switch (transactionResponse.getCode()) {
                case CONTINUE_TRANSACTION:
                    logger.debug("{} Received ContinueTransaction indicator from {}", this, peer);
                    break;
                case FINISH_TRANSACTION:
                    logger.debug("{} Received FinishTransaction indicator from {}", this, peer);
                    continueTransaction = false;
                    calculatedCRC = String.valueOf(checkedInputStream.getChecksum().getValue());
                    break;
                case CANCEL_TRANSACTION:
                    logger.info("{} Received CancelTransaction indicator from {} with explanation {}", this, peer, transactionResponse.getMessage());
                    session.rollback();
                    return 0;
                default:
                    throw new ProtocolException("Received unexpected response from peer: when expecting Continue Transaction or Finish Transaction, received" + transactionResponse);
            }
        }

        // we received a FINISH_TRANSACTION indicator. Send back a CONFIRM_TRANSACTION message
        // to peer so that we can verify that the connection is still open. This is a two-phase commit,
        // which helps to prevent the chances of data duplication. Without doing this, we may commit the
        // session and then when we send the response back to the peer, the peer may have timed out and may not
        // be listening. As a result, it will re-send the data. By doing this two-phase commit, we narrow the
        // Critical Section involved in this transaction so that rather than the Critical Section being the
        // time window involved in the entire transaction, it is reduced to a simple round-trip conversation.
        logger.debug("{} Sending CONFIRM_TRANSACTION Response Code to {}", this, peer);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, calculatedCRC);

        final Response confirmTransactionResponse = Response.read(dis);
        logger.debug("{} Received {} from {}", this, confirmTransactionResponse, peer);

        switch (confirmTransactionResponse.getCode()) {
            case CONFIRM_TRANSACTION:
                break;
            case BAD_CHECKSUM:
                session.rollback();
                throw new IOException(this + " Received a BadChecksum response from peer " + peer);
            default:
                throw new ProtocolException(this + " Received unexpected Response Code from peer " + peer + " : " + confirmTransactionResponse + "; expected 'Confirm Transaction' Response Code");
        }

        // Commit the session so that we have persisted the data
        session.commit();

        if (context.getAvailableRelationships().isEmpty()) {
            // Confirm that we received the data and the peer can now discard it but that the peer should not
            // send any more data for a bit
            logger.debug("{} Sending TRANSACTION_FINISHED_BUT_DESTINATION_FULL to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.writeResponse(dos);
        } else {
            // Confirm that we received the data and the peer can now discard it
            logger.debug("{} Sending TRANSACTION_FINISHED to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED.writeResponse(dos);
        }

        stopWatch.stop();
        final String flowFileDescription = flowFilesReceived.size() < 20 ? flowFilesReceived.toString() : flowFilesReceived.size() + " FlowFiles";
        final String uploadDataRate = stopWatch.calculateDataRate(bytesReceived);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesReceived);
        logger.info("{} Successfully received {} ({}) from {} in {} milliseconds at a rate of {}", new Object[]{
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesReceived.size();
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void shutdown(final Peer peer) {
        logger.debug("{} Shutting down with {}", this, peer);
        shutdown = true;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public void setNodeInformant(final NodeInformant nodeInformant) {
    }

    @Override
    public long getRequestExpiration() {
        return handshakenProperties.getExpirationMillis();
    }

    @Override
    public String toString() {
        String commid = handshakenProperties != null ? handshakenProperties.getCommsIdentifier() : null;
        return getClass().getSimpleName() + "[CommsID=" + commid + "]";
    }
}
