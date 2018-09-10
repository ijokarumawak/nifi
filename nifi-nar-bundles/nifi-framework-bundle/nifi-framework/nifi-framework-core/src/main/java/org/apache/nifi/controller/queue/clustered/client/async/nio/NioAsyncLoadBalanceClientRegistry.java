package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class NioAsyncLoadBalanceClientRegistry implements AsyncLoadBalanceClientRegistry {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClientRegistry.class);

    private final NioAsyncLoadBalanceClientFactory clientFactory;
    private final int clientsPerNode;

    private Map<NodeIdentifier, Set<NioAsyncLoadBalanceClient>> clientMap = new HashMap<>();
    private Set<NioAsyncLoadBalanceClient> allClients = new CopyOnWriteArraySet<>();
    private boolean running = false;

    public NioAsyncLoadBalanceClientRegistry(final NioAsyncLoadBalanceClientFactory clientFactory, final int clientsPerNode) {
        this.clientFactory = clientFactory;
        this.clientsPerNode = clientsPerNode;
    }

    @Override
    public synchronized void register(final String connectionId, final NodeIdentifier nodeId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier,
                                      final TransactionFailureCallback failureCallback, final TransactionCompleteCallback successCallback, final LoadBalanceCompression compression) {
        Set<NioAsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            clients = registerClients(nodeId);
        }

        clients.forEach(client -> client.register(connectionId, emptySupplier, flowFileSupplier, failureCallback, successCallback, compression));
        logger.debug("Registered Connection with ID {} to send to Node {}", connectionId, nodeId);
    }


    @Override
    public synchronized void unregister(final String connectionId, final NodeIdentifier nodeId) {
        final Set<NioAsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            return;
        }

        clients.forEach(client -> client.unregister(connectionId));
        logger.debug("Un-registered Connection with ID {} so that it will no longer send data to Node {}", connectionId, nodeId);
    }

    private Set<NioAsyncLoadBalanceClient> registerClients(final NodeIdentifier nodeId) {
        final Set<NioAsyncLoadBalanceClient> clients = new HashSet<>();

        for (int i=0; i < clientsPerNode; i++) {
            final NioAsyncLoadBalanceClient client = clientFactory.createClient(nodeId);
            clients.add(client);

            logger.debug("Added client {} for communicating with Node {}", client, nodeId);
        }

        clientMap.put(nodeId, clients);
        allClients.addAll(clients);

        if (running) {
            clients.forEach(NioAsyncLoadBalanceClient::start);
        }

        return clients;
    }

    public synchronized Set<NioAsyncLoadBalanceClient> getAllClients() {
        return allClients;
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        allClients.forEach(NioAsyncLoadBalanceClient::start);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }

        running = false;
        allClients.forEach(NioAsyncLoadBalanceClient::stop);
    }
}
