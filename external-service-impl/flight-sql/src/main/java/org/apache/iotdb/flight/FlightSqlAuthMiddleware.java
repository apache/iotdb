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
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flight Server middleware for handling Bearer token / Basic authentication. Supports initial login
 * via Basic auth header (username:password), returning a Bearer token. Subsequent requests use the
 * Bearer token.
 */
public class FlightSqlAuthMiddleware implements FlightServerMiddleware {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlAuthMiddleware.class);

  /** The middleware key used to retrieve this middleware in the CallContext. */
  public static final Key<FlightSqlAuthMiddleware> KEY = Key.of("flight-sql-auth-middleware");

  private final CallHeaders incomingHeaders;

  FlightSqlAuthMiddleware(CallHeaders incomingHeaders) {
    this.incomingHeaders = incomingHeaders;
  }

  /** Returns the incoming call headers for session lookup. */
  public CallHeaders getCallHeaders() {
    return incomingHeaders;
  }

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    // no-op: token is set during Handshake response, not here
  }

  @Override
  public void onCallCompleted(CallStatus status) {
    // no-op
  }

  @Override
  public void onCallErrored(Throwable err) {
    // no-op
  }

  // ===================== Factory =====================

  /** Factory that creates FlightSqlAuthMiddleware for each call. */
  public static class Factory implements FlightServerMiddleware.Factory<FlightSqlAuthMiddleware> {

    private final FlightSqlSessionManager sessionManager;

    public Factory(FlightSqlSessionManager sessionManager) {
      this.sessionManager = sessionManager;
    }

    @Override
    public FlightSqlAuthMiddleware onCallStarted(
        CallInfo callInfo, CallHeaders incomingHeaders, RequestContext context) {
      return new FlightSqlAuthMiddleware(incomingHeaders);
    }

    public FlightSqlSessionManager getSessionManager() {
      return sessionManager;
    }
  }
}
