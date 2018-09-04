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

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorizable for a component that can be scheduled by operators.
 */
public class OperationAuthorizable implements Authorizable {
    private static Logger logger = LoggerFactory.getLogger(OperationAuthorizable.class);
    private final Authorizable authorizable;

    public OperationAuthorizable(final Authorizable authorizable) {
        this.authorizable = authorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        // Need to return parent operation authorizable. E.g. /operation/processor/xxxx -> /operation/process-group/yyyy -> /run-status/process-group/root
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

    public static void authorize(final Authorizable authorizable, final Authorizer authorizer, final RequestAction requestAction, final NiFiUser user) {
        try {
            authorizable.authorize(authorizer, requestAction, user);
        } catch (AccessDeniedException e) {
            logger.debug("Authorization failed with {}. Try authorizing with OperationAuthorizable.", authorizable, e);
            // Always use WRITE action for operation.
            new OperationAuthorizable(authorizable).authorize(authorizer, RequestAction.WRITE, user);
        }

    }

    public static boolean isAuthorized(final Authorizable authorizable, final Authorizer authorizer, final RequestAction requestAction, final NiFiUser user) {
        return authorizable.isAuthorized(authorizer, requestAction, user)
                || new OperationAuthorizable(authorizable).isAuthorized(authorizer, requestAction, user);
    }

}
