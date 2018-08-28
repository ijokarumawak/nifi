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

package org.apache.nifi.web.api.dto.status;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status of a controller service.
 */
@XmlType(name = "controllerServiceStatus")
public class ControllerServiceStatusDTO implements Cloneable {
    private String groupId;
    private String id;
    private String name;
    private String runStatus;

    @ApiModelProperty("The unique ID of the process group that the ControllerService belongs to")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The unique ID of the ControllerService")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The name of the ControllerService")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty(value="The run status of the ControllerService",
            allowableValues = "Enabling, Enabled, Disabling, Disabled, Invalid")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    @Override
    public ControllerServiceStatusDTO clone() {
        final ControllerServiceStatusDTO other = new ControllerServiceStatusDTO();
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setName(getName());
        other.setRunStatus(getRunStatus());

        return other;
    }
}
