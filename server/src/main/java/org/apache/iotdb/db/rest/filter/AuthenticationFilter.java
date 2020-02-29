/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.rest.filter;

import java.lang.reflect.Method;
import java.util.Base64;
import java.util.List;
import java.util.StringTokenizer;
import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.rest.service.RestService;

public class AuthenticationFilter implements ContainerRequestFilter {

  @Context
  private ResourceInfo resourceInfo;

  private static final String AUTHORIZATION_PROPERTY = "Authorization";
  private static final String AUTHENTICATION_SCHEME = "Basic";

  @Override
  public void filter(ContainerRequestContext containerRequestContext) {
    Method method = resourceInfo.getResourceMethod();
    //Access allowed for all
    if(!method.isAnnotationPresent(PermitAll.class)) {
      //Access denied for all
      if(method.isAnnotationPresent(DenyAll.class)) {
        containerRequestContext.abortWith(Response.status(Response.Status.FORBIDDEN)
            .entity("Access blocked for all users !!").build());
        return;
      }

      //Get request headers
      final MultivaluedMap<String, String> headers = containerRequestContext.getHeaders();

      //Fetch authorization header
      final List<String> authorization = headers.get(AUTHORIZATION_PROPERTY);

      //If no authorization information present; block access
      if(authorization == null || authorization.isEmpty()) {
        containerRequestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
            .entity("You cannot access this resource").build());
        return;
      }

      //Get encoded username and password
      final String encodedUserPassword = authorization.get(0).replaceFirst(AUTHENTICATION_SCHEME + " ", "");

      //Decode username and password
      String usernameAndPassword = new String(Base64.getDecoder().decode(encodedUserPassword.getBytes()));

      //Split username and password tokens
      final StringTokenizer tokenizer = new StringTokenizer(usernameAndPassword, ":");
      final String username = tokenizer.nextToken();
      final String password = tokenizer.nextToken();


      //Is user valid?
      try {
        if(!isUserAllowed(username, password)) {
          containerRequestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
              .entity("You cannot access this resource").build());
        } else {
          RestService.getInstance().setUsername(username);
        }
      } catch (AuthException e) {
        containerRequestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
            .entity("You cannot access this resource").build());
      }
    }
  }

  private boolean isUserAllowed(String username, String password) throws AuthException {
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    return authorizer.login(username, password);
  }
}
