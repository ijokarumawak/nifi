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
package org.apache.nifi.authorization.resource;

import org.apache.nifi.authorization.Resource;

/**
 * Authorizable for a component that has run status such as 'STARTED', 'STOPPED', 'ENABLED' or 'DISABLED' ... etc.
 */
public class OperationAuthorizable implements Authorizable {
    private final Authorizable authorizable;

    public OperationAuthorizable(final Authorizable authorizable) {
        this.authorizable = authorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        // TODO: Need to return parent authorizable. E.g. /run-status/processor/xxxx -> /run-status/process-group/yyyy -> /run-status/process-group/root
        if (authorizable.getParentAuthorizable() == null) {
            return null;
        } else {
            return new OperationAuthorizable(authorizable.getParentAuthorizable());
        }
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getOperationResource(authorizable.getResource());
    }


}
