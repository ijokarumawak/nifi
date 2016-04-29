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
package org.apache.nifi.remote.io.http;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.protocol.socket.ResponseCode;

import java.io.InputStream;
import java.io.OutputStream;

public class HttpServerCommunicationsSession extends HttpCommunicationsSession {

    private String transactionId;
    private Transaction.TransactionState status = Transaction.TransactionState.TRANSACTION_STARTED;
    private ResponseCode responseCode;

    public HttpServerCommunicationsSession(InputStream inputStream, OutputStream outputStream, String transactionId){
        super();
        input.setInputStream(inputStream);
        output.setOutputStream(outputStream);
        this.transactionId = transactionId;
    }

    // HttpClientTransaction has its own status, this status is only needed by HttpFlowFileServerProtocol.
    // Because multiple HttpFlowFileServerProtocol instances have to carry on a single transaction through multiple HTTP requests, so status has to be embedded here.
    public Transaction.TransactionState getStatus() {
        return status;
    }

    public void setStatus(Transaction.TransactionState status) {
        this.status = status;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(ResponseCode responseCode) {
        this.responseCode = responseCode;
    }
}
