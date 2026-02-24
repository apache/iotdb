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

package org.apache.iotdb.flight;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Arrow Flight SQL authenticator that supports both Basic and Bearer token authentication. On the
 * first call, Basic credentials are validated and a Bearer token is returned. On subsequent calls,
 * the Bearer token is used to look up the existing session, avoiding creating a new session per
 * call.
 */
public class FlightSqlAuthHandler implements CallHeaderAuthenticator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlAuthHandler.class);
  private static final String AUTHORIZATION_HEADER = "authorization";
  private static final String BASIC_PREFIX = "Basic ";
  private static final String BEARER_PREFIX = "Bearer ";

  private final FlightSqlSessionManager sessionManager;

  public FlightSqlAuthHandler(FlightSqlSessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  @Override
  public AuthResult authenticate(CallHeaders headers) {
    Iterable<String> authHeaders;
    try {
      authHeaders = headers.getAll(AUTHORIZATION_HEADER);
    } catch (NullPointerException e) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Missing Authorization header (null header map)")
          .toRuntimeException();
    }

    // First pass: check for Bearer token (reuse existing session)
    String basicHeader = null;
    if (authHeaders == null) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Missing Authorization header")
          .toRuntimeException();
    }
    for (String authHeader : authHeaders) {
      if (authHeader.startsWith(BEARER_PREFIX)) {
        String token = authHeader.substring(BEARER_PREFIX.length());
        try {
          sessionManager.getSessionByToken(token);
          return bearerResult(token);
        } catch (SecurityException e) {
          // Bearer token invalid/expired, fall through to Basic auth
          LOGGER.debug("Bearer token invalid, falling back to Basic auth");
        }
      } else if (authHeader.startsWith(BASIC_PREFIX) && basicHeader == null) {
        basicHeader = authHeader;
      }
    }

    // Second pass: fall back to Basic auth (create new session)
    if (basicHeader != null) {
      String encoded = basicHeader.substring(BASIC_PREFIX.length());
      String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
      int colonIdx = decoded.indexOf(':');
      if (colonIdx < 0) {
        throw CallStatus.UNAUTHENTICATED
            .withDescription("Invalid Basic credentials format")
            .toRuntimeException();
      }
      String username = decoded.substring(0, colonIdx);
      String password = decoded.substring(colonIdx + 1);

      String clientId = headers.get("x-flight-sql-client-id");
      LOGGER.debug("Validating credentials for user: {}, clientId: {}", username, clientId);
      try {
        String token = sessionManager.authenticate(username, password, "unknown", clientId);
        return bearerResult(token);
      } catch (SecurityException e) {
        throw CallStatus.UNAUTHENTICATED.withDescription(e.getMessage()).toRuntimeException();
      }
    }

    throw CallStatus.UNAUTHENTICATED
        .withDescription("Missing or unsupported Authorization header")
        .toRuntimeException();
  }

  /**
   * Creates an AuthResult that sends the Bearer token back in response headers. The client's
   * ClientIncomingAuthHeaderMiddleware captures this token for use on subsequent calls.
   */
  private static AuthResult bearerResult(String token) {
    return new AuthResult() {
      @Override
      public String getPeerIdentity() {
        return token;
      }

      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        if (outgoingHeaders == null) {
          return;
        }
        try {
          outgoingHeaders.insert(AUTHORIZATION_HEADER, BEARER_PREFIX + token);
        } catch (NullPointerException e) {
          // Some CallHeaders implementations have null internal maps for certain RPCs
          LOGGER.debug("Could not append Bearer token to outgoing headers", e);
        }
      }
    };
  }
}
