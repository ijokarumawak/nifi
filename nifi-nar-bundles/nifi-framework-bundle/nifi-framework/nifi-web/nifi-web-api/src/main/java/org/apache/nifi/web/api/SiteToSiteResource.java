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
package org.apache.nifi.web.api;

import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.controller.ControllerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Path("/site-to-site")
@Api(
        value = "/site-to-site",
        description = "Provides SiteToSite API to be called by a SiteToSite client remotely."
)
public class SiteToSiteResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteResource.class);

    // TODO: Remove serviceFacade if we don't need it.
    private NiFiServiceFacade serviceFacade;
    private ControllerFacade controllerFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ResourceContext resourceContext;

    private final AtomicReference<ProcessGroup> rootGroup = new AtomicReference<>();

    /**
     * Returns the details of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    // TODO: @PreAuthorize("hasRole('ROLE_NIFI')")
    @ApiOperation(
            value = "Returns the details about this NiFi necessary to communicate via site to site",
            response = ControllerEntity.class,
            authorizations = @Authorization(value = "NiFi", type = "ROLE_NIFI")
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getPeers(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        if (properties.isClusterManager()) {
            // TODO: Get peers within this cluster.
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        InetSocketAddress apiAddress = properties.getNodeApiAddress();


        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // TODO: Determine if the connection is secured.
        PeerDTO peer = new PeerDTO();
        peer.setHostname(apiAddress.getHostName());
        peer.setPort(apiAddress.getPort());
        peer.setSecure(false);
        peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

        ArrayList<PeerDTO> peers = new ArrayList<>(1);
        peers.add(peer);

        PeersEntity entity = new PeersEntity();
        entity.setRevision(revision);
        entity.setPeers(peers);

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("ports/{id}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Transfer flow files to input port",
            response = TransactionResultEntity.class,
            authorizations = {
                    @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                    @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                    @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response transferFlowFile(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The input port id.",
                    required = true
            )
            @PathParam("id") String id,
            @Context HttpServletRequest req,
            InputStream inputStream) {

        logger.info("### Received transferFlowFile request: id=" + id + " inputStream=" + inputStream, " req=" + req);

        // Get Port
        RootGroupPort port = null;
        for(RootGroupPort p : controllerFacade.getInputPorts()){
            if(p.getIdentifier().equals(id)){
                port = p;
                break;
            }
        }

        if(port == null){
            // TODO: Illegal Argument.
            throw new IllegalArgumentException(id);
        }

        String clientHostName = req.getRemoteHost();
        int clientPort = req.getRemotePort();
        PeerDescription peerDescription = new PeerDescription(clientHostName, clientPort, req.isSecure());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CommunicationsSession communicationsSession = new HttpCommunicationsSession(inputStream, out);

        // TODO: Are those values used?
        String clusterUrl = "Unkown";
        String peerUrl = "Unkown";
        Peer peer = new Peer(peerDescription, communicationsSession, peerUrl, clusterUrl);
        HttpFlowFileServerProtocol serverProtocol = new HttpFlowFileServerProtocol();
        // TODO: get headers.
        serverProtocol.setTransferredData(new HashMap<>(), req.getContentLength());
        try {
            // TODO: this request Headers is never used.
            port.receiveFlowFiles(peer, serverProtocol, null);
        } catch (NotAuthorizedException | BadRequestException | RequestExpiredException e) {
            // TODO: error handling.
            logger.error("Failed to process the request.", e);
            return Response.serverError().build();
        }

        TransactionResultEntity entity = new TransactionResultEntity();
        return clusterContext(noCache(Response.ok(entity))).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setControllerFacade(ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }
}
