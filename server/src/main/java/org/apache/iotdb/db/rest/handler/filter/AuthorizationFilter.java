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
package org.apache.iotdb.db.rest.handler.filter;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.rest.handler.model.User;
import org.apache.iotdb.db.rest.model.ResponseResult;

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

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    IAuthorizer authorizer;
    if (!"OPTIONS".equals(containerRequestContext.getMethod())
        && !containerRequestContext.getUriInfo().getPath().equals("swagger.json")) {
      boolean isLogin;
      String authzHeader = containerRequestContext.getHeaderString("authorization");
      String decoded = Base64.decodeAsString(authzHeader.replace("Basic ", ""));
      String[] split = decoded.split(":");
      User user = new User();
      user.setUsername(split[0]);
      if (split.length != 2) {
        Response resp =
            Response.status(Status.BAD_REQUEST)
                .type(MediaType.APPLICATION_JSON)
                .entity(getResponseResult(13004, "Authorization is illegal"))
                .build();
        containerRequestContext.abortWith(resp);
      } else {
        user.setPassword(split[1]);
        BasicSecurityContext basicSecurityContext =
            new BasicSecurityContext(user, containerRequestContext.getSecurityContext().isSecure());
        containerRequestContext.setSecurityContext(basicSecurityContext);
        try {
          authorizer = BasicAuthorizer.getInstance();
          isLogin = authorizer.login(split[0], split[1]);
          if (!isLogin) {
            Response resp =
                Response.status(Status.OK)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(getResponseResult(13200, "username or passowrd is incorrect!"))
                    .build();
            containerRequestContext.abortWith(resp);
          }
        } catch (AuthException e) {
          LOGGER.warn(e.getMessage(), e);
          Response resp =
              Response.status(Status.INTERNAL_SERVER_ERROR)
                  .type(MediaType.APPLICATION_JSON)
                  .entity(getResponseResult(Status.UNAUTHORIZED.getStatusCode(), e.getMessage()))
                  .build();
          containerRequestContext.abortWith(resp);
        }
      }
    }
  }

  private ResponseResult getResponseResult(int code, String message) {
    ResponseResult responseResult = new ResponseResult();
    responseResult.setCode(code);
    responseResult.setMessage(message);
    return responseResult;
  }
}
