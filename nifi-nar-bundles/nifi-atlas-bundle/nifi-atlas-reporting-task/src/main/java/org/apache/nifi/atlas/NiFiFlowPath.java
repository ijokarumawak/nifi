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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NiFiFlowPath implements AtlasProcess {
    private final List<String> processComponentIds = new ArrayList<>();

    private final String id;
    private final Set<AtlasObjectId> inputs = new HashSet<>();
    private final Set<AtlasObjectId> outputs = new HashSet<>();

    private String atlasGuid;
    private String name;
    private String groupId;

    private AtlasEntity exEntity;

    public NiFiFlowPath(String id) {
        this.id = id;
    }
    public NiFiFlowPath(String id, long lineageHash) {
        this.id =  id + "::" + lineageHash;
    }

    public AtlasEntity getExEntity() {
        return exEntity;
    }

    public void setExEntity(AtlasEntity exEntity) {
        this.exEntity = exEntity;
        this.atlasGuid = exEntity.getGuid();
    }

    public String getAtlasGuid() {
        return atlasGuid;
    }

    public void setAtlasGuid(String atlasGuid) {
        this.atlasGuid = atlasGuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void addProcessor(String processorId) {
        processComponentIds.add(processorId);
    }

    public Set<AtlasObjectId> getInputs() {
        return inputs;
    }

    public Set<AtlasObjectId> getOutputs() {
        return outputs;
    }

    public List<String> getProcessComponentIds() {
        return processComponentIds;
    }

    public String getId() {
        return id;
    }

    public String createDeepLinkURL(String nifiUrl) {
        // Remove lineage hash part.
        final String componentId = id.split("::")[0];
        return componentId.equals(groupId)
                // This path represents the root path of a process group.
                ? String.format("%s?processGroupId=%s", nifiUrl, groupId)
                // This path represents a partial flow within a process group consists of processors.
                : String.format("%s?processGroupId=%s&componentIds=%s", nifiUrl, groupId, componentId);
    }

    @Override
    public String toString() {
        return "NiFiFlowPath{" +
                "name='" + name + '\'' +
                ", inputs=" + inputs +
                ", outputs=" + outputs +
                ", processComponentIds=" + processComponentIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiFiFlowPath that = (NiFiFlowPath) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
