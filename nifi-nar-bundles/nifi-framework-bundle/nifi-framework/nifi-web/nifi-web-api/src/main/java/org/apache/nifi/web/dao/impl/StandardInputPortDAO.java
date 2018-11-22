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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StandardInputPortDAO extends ComponentDAO implements PortDAO {

    private FlowController flowController;

    private Port locatePort(final String portId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        Port port = rootGroup.findInputPort(portId);

        if (port == null) {
            port = rootGroup.findOutputPort(portId);
        }

        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to find port with id '%s'.", portId));
        } else {
            return port;
        }
    }

    @Override
    public boolean hasPort(String portId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        return rootGroup.findInputPort(portId) != null || rootGroup.findOutputPort(portId) != null;
    }

    @Override
    public Port createPort(String groupId, PortDTO portDTO) {
        if (isNotNull(portDTO.getParentGroupId()) && !flowController.getFlowManager().areGroupsSame(groupId, portDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the InputPort is being added.");
        }

        // ensure the name has been specified
        if (portDTO.getName() == null) {
            throw new IllegalArgumentException("Port name must be specified.");
        }

        // get the desired group
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // determine if this is the root group
        Port port;
        if (group.getParent() == null) {
            port = flowController.getFlowManager().createRootGroupInputPort(portDTO.getId(), portDTO.getName());
        } else {
            port = flowController.getFlowManager().createLocalInputPort(portDTO.getId(), portDTO.getName());
            flowController.getFlowManager().setRemoteAccessibility(port, Boolean.TRUE.equals(portDTO.isAllowRemoteAccess()));
        }

        // ensure we can perform the update before we add the port to the flow
        verifyUpdate(port, portDTO);

        // configure
        if (portDTO.getPosition() != null) {
            port.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        port.setComments(portDTO.getComments());

        // add the port
        group.addInputPort(port);
        return port;
    }

    @Override
    public Port getPort(String portId) {
        return locatePort(portId);
    }

    @Override
    public Set<Port> getPorts(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getInputPorts();
    }

    @Override
    public void verifyUpdate(PortDTO portDTO) {
        final Port inputPort = locatePort(portDTO.getId());
        verifyUpdate(inputPort, portDTO);
    }

    private void verifyUpdate(final Port inputPort, final PortDTO portDTO) {
        verifyUpdate(inputPort, portDTO, analyzePortTypeChange(inputPort, portDTO));
    }

    private void verifyUpdate(final Port inputPort, final PortDTO portDTO, final PortTypeChange portTypeChange) {
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(inputPort.getScheduledState())) {
                // perform the appropriate action
                switch (purposedScheduledState) {
                    case RUNNING:
                        inputPort.verifyCanStart();
                        break;
                    case STOPPED:
                        switch (inputPort.getScheduledState()) {
                            case RUNNING:
                                inputPort.verifyCanStop();
                                break;
                            case DISABLED:
                                inputPort.verifyCanEnable();
                                break;
                        }
                        break;
                    case DISABLED:
                        inputPort.verifyCanDisable();
                        break;
                }
            }
        }

        // see what's be modified
        if (isAnyNotNull(portDTO.getUserAccessControl(),
                portDTO.getGroupAccessControl(),
                portDTO.getConcurrentlySchedulableTaskCount(),
                portDTO.getName(),
                portDTO.getComments())) {

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(inputPort, portDTO, portTypeChange);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }

            // ensure the port can be updated
            inputPort.verifyCanUpdate();
        }

    }

    private List<String> validateProposedConfiguration(final Port port, final PortDTO portDTO,
                                                       final PortTypeChange portTypeChange) {
        List<String> validationErrors = new ArrayList<>();

        if (isNotNull(portDTO.getName()) && portDTO.getName().trim().isEmpty()) {
            validationErrors.add("Port name cannot be blank.");
        }
        if (isNotNull(portDTO.getConcurrentlySchedulableTaskCount()) && portDTO.getConcurrentlySchedulableTaskCount() <= 0) {
            validationErrors.add("Concurrent tasks must be a positive integer.");
        }

        if (portTypeChange.willBePublic) {
            final String portName = isNotNull(portDTO.getName()) ? portDTO.getName() : port.getName();
            if (flowController.getFlowManager().getPublicInputPorts().stream()
                    .anyMatch(p -> portName.equals(p.getName()))) {
                throw new IllegalStateException("Public port name should be unique in the entire flow.");
            }
        }


        return validationErrors;
    }

    private class PortTypeChange {
        private final boolean isPublic;
        private final boolean willBePublic;

        private PortTypeChange(boolean isPublic, boolean willBePublic) {
            this.isPublic = isPublic;
            this.willBePublic = willBePublic;
        }

        private boolean isLocalToPublic() {
            return !isPublic && willBePublic;
        }

        private boolean isPublicToLocal() {
            return isPublic && !willBePublic;
        }
    }

    private PortTypeChange analyzePortTypeChange(final Port port, final PortDTO portDTO) {
        // handle Port type change.
        final boolean isPublicPort = port.isAllowRemoteAccess();
        final boolean isRootGroup = port.getProcessGroup() == null || port.getProcessGroup().getParent() == null;
        final boolean willAllowRemoteAccess = portDTO.isAllowRemoteAccess() != null ? portDTO.isAllowRemoteAccess() : isPublicPort;
        final boolean willBePublicPort = (willAllowRemoteAccess || isRootGroup);
        return new PortTypeChange(isPublicPort, willBePublicPort);
    }

    @Override
    public Port updatePort(PortDTO portDTO) {
        Port inputPort = locatePort(portDTO.getId());
        final ProcessGroup processGroup = inputPort.getProcessGroup();
        final PortTypeChange portTypeChange = analyzePortTypeChange(inputPort, portDTO);

        // ensure we can do this update
        verifyUpdate(inputPort, portDTO, portTypeChange);

        // handle Port type change.
        final boolean localToPublic = portTypeChange.isLocalToPublic();
        final boolean publicToLocal = portTypeChange.isPublicToLocal();

        if (localToPublic || publicToLocal) {
            // recreate the port instance.
            flowController.getFlowManager().setRemoteAccessibility(inputPort, localToPublic);
        }

        // handle state transition
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(inputPort.getScheduledState())) {
                try {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            processGroup.startInputPort(inputPort);
                            break;
                        case STOPPED:
                            switch (inputPort.getScheduledState()) {
                                case RUNNING:
                                    processGroup.stopInputPort(inputPort);
                                    break;
                                case DISABLED:
                                    processGroup.enableInputPort(inputPort);
                                    break;
                            }
                            break;
                        case DISABLED:
                            processGroup.disableInputPort(inputPort);
                            break;
                    }
                } catch (IllegalStateException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                }
            }
        }

        if (inputPort.isAllowRemoteAccess()) {
            final PublicPort publicPort = inputPort.getPublicPort();
            if (isNotNull(portDTO.getGroupAccessControl())) {
                publicPort.setGroupAccessControl(portDTO.getGroupAccessControl());
            }
            if (isNotNull(portDTO.getUserAccessControl())) {
                publicPort.setUserAccessControl(portDTO.getUserAccessControl());
            }
        }

        // update the port
        final String name = portDTO.getName();
        final String comments = portDTO.getComments();
        final Integer concurrentTasks = portDTO.getConcurrentlySchedulableTaskCount();
        if (isNotNull(portDTO.getPosition())) {
            inputPort.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        if (isNotNull(name)) {
            inputPort.setName(name);
        }
        if (isNotNull(comments)) {
            inputPort.setComments(comments);
        }
        if (isNotNull(concurrentTasks)) {
            inputPort.setMaxConcurrentTasks(concurrentTasks);
        }

        processGroup.onComponentModified();
        return inputPort;
    }

    @Override
    public void verifyDelete(final String portId) {
        final Port inputPort = locatePort(portId);
        inputPort.verifyCanDelete();
    }

    @Override
    public void deletePort(final String portId) {
        final Port inputPort = locatePort(portId);
        inputPort.getProcessGroup().removeInputPort(inputPort);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
