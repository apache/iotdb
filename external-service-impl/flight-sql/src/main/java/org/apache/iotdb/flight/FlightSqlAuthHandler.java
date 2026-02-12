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

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arrow Flight SQL credential validator using Arrow's built-in auth2 framework. Validates
 * username/password credentials via IoTDB's SessionManager and returns a Bearer token string as the
 * peer identity for subsequent requests.
 *
 * <p>Used with {@link BasicCallHeaderAuthenticator} and {@link
 * org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator} to provide Basic → Bearer token
 * authentication flow.
 */
public class FlightSqlAuthHandler implements BasicCallHeaderAuthenticator.CredentialValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlAuthHandler.class);
  private final FlightSqlSessionManager sessionManager;

  public FlightSqlAuthHandler(FlightSqlSessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  @Override
  public org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult validate(
      String username, String password) {
    LOGGER.debug("Validating credentials for user: {}", username);

    try {
      String token = sessionManager.authenticate(username, password, "unknown");
      // Return the token as the peer identity; GeneratedBearerTokenAuthenticator
      // wraps it in a Bearer token and sets it in the response header.
      return () -> token;
    } catch (SecurityException e) {
      throw CallStatus.UNAUTHENTICATED.withDescription(e.getMessage()).toRuntimeException();
    }
  }
}
