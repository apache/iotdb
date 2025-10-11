/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.filter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.session.RestClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.servlet.annotation.WebFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Base64;
import java.util.UUID;

@WebFilter("/*")
@Provider
public class AuthorizationFilter implements ContainerRequestFilter, ContainerResponseFilter {

  private final UserCache userCache = UserCache.getInstance();
  IoTDBRestServiceConfig config = IoTDBRestServiceDescriptor.getInstance().getConfig();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  public AuthorizationFilter() throws AuthException {
    // do nothing
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {

    if ("OPTIONS".equals(containerRequestContext.getMethod())
        || "ping".equals(containerRequestContext.getUriInfo().getPath())
        || (config.isEnableSwagger()
            && "swagger.json".equals(containerRequestContext.getUriInfo().getPath()))) {
      return;
    } else if (!config.isEnableSwagger()
        && "swagger.json".equals(containerRequestContext.getUriInfo().getPath())) {
      Response resp =
          Response.status(Status.NOT_FOUND).type(MediaType.APPLICATION_JSON).entity("").build();
      containerRequestContext.abortWith(resp);
      return;
    }

    String authorizationHeader = containerRequestContext.getHeaderString("authorization");
    if (authorizationHeader == null) {
      Response resp =
          Response.status(Status.UNAUTHORIZED)
              .type(MediaType.APPLICATION_JSON)
              .entity(
                  new ExecutionStatus()
                      .code(TSStatusCode.INIT_AUTH_ERROR.getStatusCode())
                      .message(TSStatusCode.INIT_AUTH_ERROR.name()))
              .build();
      containerRequestContext.abortWith(resp);
      return;
    }
    User user = userCache.getUser(authorizationHeader);
    if (user == null) {
      user = checkLogin(containerRequestContext, authorizationHeader);
      if (user == null) {
        return;
      } else {
        userCache.setUser(authorizationHeader, user);
      }
    }
    String sessionid = UUID.randomUUID().toString();
    if (SESSION_MANAGER.getCurrSession() == null) {
      RestClientSession restClientSession = new RestClientSession(sessionid);
      restClientSession.setUsername(user.getUsername());
      SESSION_MANAGER.registerSession(restClientSession);
      SESSION_MANAGER.supplySession(
          SESSION_MANAGER.getCurrSession(),
          user.getUserId(),
          user.getUsername(),
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0);
    }
    BasicSecurityContext basicSecurityContext =
        new BasicSecurityContext(
            user, IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableHttps());
    containerRequestContext.setSecurityContext(basicSecurityContext);
  }

  private User checkLogin(
      ContainerRequestContext containerRequestContext, String authorizationHeader) {

    byte[] decodedBytes = Base64.getDecoder().decode(authorizationHeader.replace("Basic ", ""));
    String decoded = new String(decodedBytes);

    // todo: support special chars in username and password
    String[] split = decoded.split(":");
    if (split.length != 2) {
      Response resp =
          Response.status(Status.BAD_REQUEST)
              .type(MediaType.APPLICATION_JSON)
              .entity(
                  new ExecutionStatus()
                      .code(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode())
                      .message("Illegal format of authorization header."))
              .build();
      containerRequestContext.abortWith(resp);
      return null;
    }

    User user = new User();
    user.setUsername(split[0]);
    user.setPassword(split[1]);
    user.setUserId(AuthorityChecker.getUserId(split[0]).orElse(-1L));
    TSStatus tsStatus = AuthorityChecker.checkUser(split[0], split[1]);
    if (tsStatus.code != 200) {
      Response resp =
          Response.status(Status.UNAUTHORIZED)
              .type(MediaType.APPLICATION_JSON)
              .entity(
                  new ExecutionStatus()
                      .code(TSStatusCode.WRONG_LOGIN_PASSWORD.getStatusCode())
                      .message(TSStatusCode.WRONG_LOGIN_PASSWORD.name()))
              .build();
      containerRequestContext.abortWith(resp);
      return null;
    }
    return user;
  }

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    if (SESSION_MANAGER.getCurrSession() != null
        && SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()) != null) {
      SESSION_MANAGER.removeCurrSession();
    }
  }
}
