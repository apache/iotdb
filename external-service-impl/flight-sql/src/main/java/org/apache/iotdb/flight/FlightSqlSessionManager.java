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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.arrow.flight.CallHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.ZoneId;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Manages Arrow Flight SQL client sessions using Bearer token authentication. Maps Bearer tokens to
 * IoTDB IClientSession objects with a TTL-based Caffeine cache.
 */
public class FlightSqlSessionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSqlSessionManager.class);
  private static final String AUTHORIZATION_HEADER = "authorization";
  private static final String BEARER_PREFIX = "Bearer ";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final SessionManager sessionManager = SessionManager.getInstance();

  /** Cache of Bearer token -> IClientSession with configurable TTL. */
  private final Cache<String, IClientSession> tokenCache;

  /** Cache of username -> Bearer token for session reuse with Basic auth on every call. */
  private final Cache<String, String> userTokenCache;

  public FlightSqlSessionManager(long sessionTimeoutMinutes) {
    this.tokenCache =
        Caffeine.newBuilder()
            .expireAfterAccess(sessionTimeoutMinutes, TimeUnit.MINUTES)
            .removalListener(
                (String token, IClientSession session, RemovalCause cause) -> {
                  if (session != null && cause != RemovalCause.REPLACED) {
                    LOGGER.info("Flight SQL session expired: {}, cause: {}", session, cause);
                    try {
                      sessionManager.closeSession(
                          session,
                          queryId ->
                              org.apache.iotdb.db.queryengine.plan.Coordinator.getInstance()
                                  .cleanupQueryExecution(queryId),
                          false);
                    } catch (Exception e) {
                      LOGGER.error("Error closing expired session", e);
                    }
                  }
                })
            .build();
    this.userTokenCache =
        Caffeine.newBuilder().expireAfterAccess(sessionTimeoutMinutes, TimeUnit.MINUTES).build();
  }

  /**
   * Authenticates a user with username/password and returns a Bearer token.
   *
   * @param username the username
   * @param password the password
   * @param clientAddress the client's IP address
   * @return the Bearer token if authentication succeeds
   * @throws SecurityException if authentication fails
   */
  public String authenticate(String username, String password, String clientAddress) {
    // Check if this user already has an active session (reuse it)
    String existingToken = userTokenCache.getIfPresent(username);
    if (existingToken != null && tokenCache.getIfPresent(existingToken) != null) {
      return existingToken;
    }

    // Verify credentials (REST pattern)
    try {
      org.apache.iotdb.common.rpc.thrift.TSStatus status =
          AuthorityChecker.checkUser(username, password);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Authentication failed for client: {}", clientAddress);
        throw new SecurityException("Authentication failed: wrong username or password");
      }
    } catch (SecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new SecurityException("Authentication failed", e);
    }

    // Create and register session (REST pattern)
    IClientSession session = new InternalClientSession("FlightSQL-" + clientAddress);
    session.setSqlDialect(IClientSession.SqlDialect.TABLE);
    sessionManager.registerSession(session);

    long userId = AuthorityChecker.getUserId(username).orElse(-1L);
    sessionManager.supplySession(
        session, userId, username, ZoneId.systemDefault(), IoTDBConstant.ClientVersion.V_1_0);

    // Generate cryptographically secure Bearer token (32 bytes = 256 bits)
    byte[] tokenBytes = new byte[32];
    SECURE_RANDOM.nextBytes(tokenBytes);
    String token = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
    tokenCache.put(token, session);
    userTokenCache.put(username, token);
    LOGGER.info("Flight SQL authentication successful for client: {}", clientAddress);
    return token;
  }

  /**
   * Retrieves the IClientSession associated with a Bearer token from request headers.
   *
   * @param headers the Call headers containing the Authorization header
   * @return the associated IClientSession
   * @throws SecurityException if the token is invalid or expired
   */
  public IClientSession getSession(CallHeaders headers) {
    String authHeader = headers.get(AUTHORIZATION_HEADER);
    if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
      throw new SecurityException("Missing or invalid Authorization header");
    }
    String token = authHeader.substring(BEARER_PREFIX.length());
    return getSessionByToken(token);
  }

  /**
   * Retrieves the IClientSession associated with a Bearer token.
   *
   * @param token the Bearer token
   * @return the associated IClientSession
   * @throws SecurityException if the token is invalid or expired
   */
  public IClientSession getSessionByToken(String token) {
    IClientSession session = tokenCache.getIfPresent(token);
    if (session == null) {
      throw new SecurityException("Invalid or expired Bearer token");
    }
    return session;
  }

  /** Invalidates all sessions and cleans up resources. */
  public void close() {
    tokenCache.invalidateAll();
    tokenCache.cleanUp();
  }
}
