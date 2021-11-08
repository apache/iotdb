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
package org.apache.iotdb.db.rest.filter;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.rest.model.ResponseResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.glassfish.jersey.internal.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.annotation.WebFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

@WebFilter("/*")
@Provider
public class AuthorizationFilter implements ContainerRequestFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationFilter.class);

  private final IAuthorizer authorizer = BasicAuthorizer.getInstance();
  private final UserCache userCache = UserCache.getInstance();

  public AuthorizationFilter() throws AuthException {}

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    // todo
    if ("OPTIONS".equals(containerRequestContext.getMethod())
        || containerRequestContext.getUriInfo().getPath().equals("swagger.json")) {
      return;
    }

    String authorizationHeader = containerRequestContext.getHeaderString("authorization");
    User user = userCache.getUser(authorizationHeader);
    if (user == null) {
      user = checkLogin(containerRequestContext, authorizationHeader);
      if (user == null) {
        return;
      } else {
        userCache.setUser(authorizationHeader, user);
      }
    }

    BasicSecurityContext basicSecurityContext =
        new BasicSecurityContext(user, containerRequestContext.getSecurityContext().isSecure());
    containerRequestContext.setSecurityContext(basicSecurityContext);
  }

  private ResponseResult getResponseResult(int code, String message) {
    ResponseResult responseResult = new ResponseResult();
    responseResult.setCode(code);
    responseResult.setMessage(message);
    return responseResult;
  }

  private User checkLogin(
      ContainerRequestContext containerRequestContext, String authorizationHeader) {

    String decoded = Base64.decodeAsString(authorizationHeader.replace("Basic ", ""));
    // todo: support special chars in username and password
    String[] split = decoded.split(":");
    if (split.length != 2) {
      Response resp =
          Response.status(Status.BAD_REQUEST)
              .type(MediaType.APPLICATION_JSON)
              .entity(
                  getResponseResult(
                      TSStatusCode.SYSTEM_CHECK_ERROR.getStatusCode(),
                      TSStatusCode.SYSTEM_CHECK_ERROR.name()))
              .build();
      containerRequestContext.abortWith(resp);
      return null;
    }

    User user = new User();
    user.setUsername(split[0]);
    user.setPassword(split[1]);
    try {
      if (!authorizer.login(split[0], split[1])) {
        Response resp =
            Response.status(Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(
                    getResponseResult(
                        TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR.getStatusCode(),
                        TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR.name()))
                .build();
        containerRequestContext.abortWith(resp);
      }
    } catch (AuthException e) {
      LOGGER.warn(e.getMessage(), e);
      Response resp =
          Response.status(Status.INTERNAL_SERVER_ERROR)
              .type(MediaType.APPLICATION_JSON)
              .entity(
                  getResponseResult(
                      TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage()))
              .build();
      containerRequestContext.abortWith(resp);
    }
    return user;
  }
}
