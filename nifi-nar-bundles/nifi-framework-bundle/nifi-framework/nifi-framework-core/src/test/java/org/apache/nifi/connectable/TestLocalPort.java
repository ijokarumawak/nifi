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
package org.apache.nifi.connectable;

import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.PublicPort;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestLocalPort {

    @Test
    public void testInvalidLocalInputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Input1",
            null, ConnectableType.INPUT_PORT, null);

        assertFalse(port.isValid());
    }

    @Test
    public void testValidLocalInputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Input1",
            null, ConnectableType.INPUT_PORT, null);

        // Add an outgoing relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(port)
            .destination(mock(Connectable.class))
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        assertTrue(port.isValid());
    }

    @Test
    public void testInvalidPublicLocalInputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Input1",
            null, ConnectableType.INPUT_PORT, null);

        port.setPublicPort(mock(PublicPort.class));

        assertFalse(port.isValid());
    }

    @Test
    public void testValidPublicLocalInputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Input1",
            null, ConnectableType.INPUT_PORT, null);

        port.setPublicPort(mock(PublicPort.class));

        // Add an outgoing relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(port)
            .destination(mock(Connectable.class))
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        assertTrue(port.isValid());
    }

    @Test
    public void testInvalidLocalOutputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Output1",
            null, ConnectableType.OUTPUT_PORT, null);

        assertFalse(port.isValid());
    }

    @Test
    public void testValidLocalOutputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Output1",
            null, ConnectableType.OUTPUT_PORT, null);

        // Add an outgoing relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(port)
            .destination(mock(Connectable.class))
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        assertTrue(port.isValid());
    }

    @Test
    public void testValidPublicLocalOutputPort() {
        final LocalPort port = new LocalPort("local-port-1", "Output1",
            null, ConnectableType.OUTPUT_PORT, null);

        // If this is a public port, outgoing relationship is not required.
        port.setPublicPort(mock(PublicPort.class));

        assertTrue(port.isValid());
    }

}
