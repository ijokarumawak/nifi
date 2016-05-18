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

import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.protocol.FlowFileTransaction;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HttpRemoteSiteListener implements RemoteSiteListener {

    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteSiteListener.class);
    private static final int TRANSACTION_TTL_SEC = 30;
    private static HttpRemoteSiteListener instance;

    private final Map<String, FlowFileTransaction> transactions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService taskExecutor;
    private ProcessGroup rootGroup;
    private ScheduledFuture<?> transactionMaintenanceTask;

    private HttpRemoteSiteListener() {
        super();
        taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName("NiFi Http Site-to-Site Connection Transaction Maintenance");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public static HttpRemoteSiteListener getInstance() {
        if (instance == null) {
            synchronized (HttpRemoteSiteListener.class) {
                if (instance == null) {
                    instance = new HttpRemoteSiteListener();
                }
            }
        }
        return instance;
    }

    @Override
    public void setRootGroup(ProcessGroup rootGroup) {
        this.rootGroup = rootGroup;
    }

    public void setupServerProtocol(HttpFlowFileServerProtocol serverProtocol) {
        serverProtocol.setRootProcessGroup(rootGroup);
    }

    @Override
    public void start() throws IOException {
        transactionMaintenanceTask = taskExecutor.scheduleWithFixedDelay(() -> {

            int originalSize = transactions.size();
            logger.debug("Transaction maintenance task started.");
            try {
                Set<String> transactionIds = transactions.keySet().stream().collect(Collectors.toSet());
                transactionIds.stream().filter(tid -> !isTransactionActive(tid))
                    .forEach(tid -> {
                        FlowFileTransaction t = transactions.remove(tid);
                        if (t == null) {
                            logger.debug("The transaction is already finalized. transactionId={}", tid);
                        } else {
                            logger.info("Expiring a transaction. transactionId={}", tid);
                            if(t.getSession() != null){
                                logger.info("Expiring a transaction, rollback its session. transactionId={}", tid);
                                try {
                                    t.getSession().rollback();
                                } catch (Exception e) {
                                    // Swallow exception so that it can keep expiring other transactions.
                                    logger.error("Failed to rollback. transactionId={}", tid, e);
                                }
                            }
                        }
                    });
            } catch (Exception e) {
                // Swallow exception so that this thread can keep working.
                logger.error("An exception occurred while maintaining transactions", e);
            }
            logger.info("Transaction maintenance task finished. originalSize={}, currentSize={}", originalSize, transactions.size());

        }, 0, TRANSACTION_TTL_SEC / 2, TimeUnit.SECONDS);
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public void stop() {
        if(transactionMaintenanceTask != null) {
            logger.debug("Stopping transactionMaintenanceTask...");
            transactionMaintenanceTask.cancel(true);
        }
    }

    public String createTransaction() {
        final String transactionId = UUID.randomUUID().toString();
        transactions.put(transactionId, new FlowFileTransaction());
        logger.debug("Created a new transaction: {}", transactionId);
        return transactionId;
    }

    public boolean isTransactionActive(final String transactionId) {
        FlowFileTransaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return false;
        }
        if (isTransactionExpired(transaction)) {
            return false;
        }
        return true;
    }

    private boolean isTransactionExpired(FlowFileTransaction transaction) {
        long elapsedSec = transaction.getStopWatch().getElapsed(TimeUnit.SECONDS);
        if (elapsedSec >= TRANSACTION_TTL_SEC) {
            return true;
        }
        return false;
    }

    public void proceedTransaction(final String transactionId, final FlowFileTransaction transaction) throws IllegalStateException {
        FlowFileTransaction currentTransaction = finalizeTransaction(transactionId);
        if (currentTransaction.getSession() != null) {
            throw new IllegalStateException("Transaction has already been processed. It can only be finalized. transactionId=" + transactionId);
        }
        if (transaction.getSession() == null) {
            throw new IllegalStateException("Passed transaction is not associated any session yet, can not proceed. transactionId=" + transactionId);
        }
        logger.debug("Proceeding a transaction: {}", transactionId);
        transactions.put(transactionId, transaction);
    }

    public FlowFileTransaction finalizeTransaction(final String transactionId) throws IllegalStateException {
        if(!isTransactionActive(transactionId)){
            throw new IllegalStateException("Transaction was not found or not active anymore. transactionId=" + transactionId);
        }
        FlowFileTransaction transaction = transactions.remove(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction was not found anymore. It's already finalized or expired. transactionId=" + transactionId);
        }
        logger.debug("Finalized a transaction: {}", transactionId);
        return transaction;
    }

}
