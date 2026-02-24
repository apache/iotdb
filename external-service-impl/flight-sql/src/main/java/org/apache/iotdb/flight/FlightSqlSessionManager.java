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
  private static final int MAX_CLIENT_ID_LENGTH = 64;
  private static final int MAX_SESSIONS = 1000;
  private static final java.util.regex.Pattern CLIENT_ID_PATTERN =
      java.util.regex.Pattern.compile("^[a-zA-Z0-9\\-]+$");

  private final SessionManager sessionManager = SessionManager.getInstance();

  /** Cache of Bearer token -> IClientSession with configurable TTL. */
  private final Cache<String, IClientSession> tokenCache;

  /**
   * Cache of (username@clientId) -> Bearer token for session reuse. Avoids repeated session
   * creation on every RPC — necessary because the Arrow Flight client middleware does not always
   * cache the Bearer token, causing Basic auth to be re-sent on every call.
   *
   * <p>Keyed by {@code username@clientId} where clientId comes from the {@code
   * x-flight-sql-client-id} header. This ensures different logical clients (even with the same
   * username) get independent sessions with separate USE database contexts. If no clientId header
   * is present, falls back to username-only keying (shared session).
   */
  private final Cache<String, String> clientSessionCache;

  public FlightSqlSessionManager(long sessionTimeoutMinutes) {
    this.tokenCache =
        Caffeine.newBuilder()
            .maximumSize(MAX_SESSIONS)
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
    this.clientSessionCache =
        Caffeine.newBuilder()
            .maximumSize(MAX_SESSIONS)
            .expireAfterAccess(sessionTimeoutMinutes, TimeUnit.MINUTES)
            .build();
  }

  /**
   * Authenticates a user with username/password and returns a Bearer token.
   *
   * @param username the username
   * @param password the password
   * @param clientAddress the client's IP address (for logging)
   * @param clientId optional client identifier from x-flight-sql-client-id header (may be null)
   * @return the Bearer token if authentication succeeds
   * @throws SecurityException if authentication fails
   */
  public String authenticate(
      String username, String password, String clientAddress, String clientId) {
    // NOTE: We intentionally do NOT call SessionManager.login() here because it performs
    // blocking I/O that is incompatible with directExecutor() on the Netty event loop:
    //   - DataNodeAuthUtils.checkPasswordExpiration: executes SELECT via Coordinator
    //   - AuthorityChecker.getUserId: sends RPC to ConfigNode on cache miss
    // Blocking the event loop corrupts HTTP/2 connection state and causes "end-of-stream
    // mid-frame" errors on subsequent RPCs.
    //
    // Functional gaps vs SessionManager.login():
    //   - Password expiration checks (requires Coordinator query)
    //   - Login lock / brute-force protection (LoginLockManager is in-memory but keys
    //     by userId; AuthorityChecker.getUserId() is a blocking RPC, so we cannot obtain
    //     a correct userId without risking event loop stalls)
    //
    // Risk: AuthorityChecker.checkUser() may perform a one-time blocking RPC to ConfigNode
    // on cache miss (ClusterAuthorityFetcher.login). After the first successful auth, the
    // credential is cached locally, and clientSessionCache avoids repeated authenticate()
    // calls for the same client.
    //
    // TODO: Support password expiration and login lock. This requires either:
    //   (a) async auth support in Arrow Flight (not yet available), or
    //   (b) resolving the Netty classpath conflict so directExecutor() is no longer needed.

    // Always verify credentials — never skip password verification even if a cached
    // session exists for this client.
    org.apache.iotdb.common.rpc.thrift.TSStatus status;
    try {
      status = AuthorityChecker.checkUser(username, password);
    } catch (Exception e) {
      throw new SecurityException("Authentication failed", e);
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Authentication failed for client: {}", clientAddress);
      throw new SecurityException("Authentication failed: wrong username or password");
    }

    // Reuse existing session for this client.
    // Key uses \0 (null byte) delimiter — cannot appear in usernames or HTTP headers,
    // so the mapping (username, clientId) -> cacheKey is injective (no collisions).
    String validClientId = validateClientId(clientId);
    String cacheKey = validClientId != null ? username + "\0" + validClientId : username;
    String existingToken = clientSessionCache.getIfPresent(cacheKey);
    if (existingToken != null && tokenCache.getIfPresent(existingToken) != null) {
      return existingToken;
    }

    // Create session. Do NOT call registerSession() — it sets a ThreadLocal (currSession)
    // designed for the client-thread model (Thrift). gRPC with directExecutor() runs all
    // handlers on the Netty event loop, so ThreadLocal-based session tracking would pollute.
    IClientSession session = new InternalClientSession("FlightSQL-" + clientAddress);
    session.setSqlDialect(IClientSession.SqlDialect.TABLE);
    // Pass -1L for userId — getUserId() sends blocking RPC to ConfigNode.
    sessionManager.supplySession(
        session,
        -1L,
        username,
        java.time.ZoneId.systemDefault(),
        IoTDBConstant.ClientVersion.V_1_0);

    // Generate cryptographically secure Bearer token (32 bytes = 256 bits)
    byte[] tokenBytes = new byte[32];
    SECURE_RANDOM.nextBytes(tokenBytes);
    String token = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
    tokenCache.put(token, session);
    clientSessionCache.put(cacheKey, token);
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

  /**
   * Validates the client ID from the x-flight-sql-client-id header. Returns the validated clientId,
   * or null if the header was absent (null/empty). Non-empty invalid clientIds are rejected
   * (fail-closed) to prevent silent fallback to shared username-only sessions, which would break
   * USE database isolation.
   *
   * @throws SecurityException if clientId is non-empty but invalid (too long or bad characters)
   */
  private static String validateClientId(String clientId) {
    if (clientId == null || clientId.isEmpty()) {
      return null;
    }
    if (clientId.length() > MAX_CLIENT_ID_LENGTH) {
      throw new SecurityException(
          "Client ID exceeds maximum length of " + MAX_CLIENT_ID_LENGTH + " characters");
    }
    if (!CLIENT_ID_PATTERN.matcher(clientId).matches()) {
      throw new SecurityException(
          "Client ID contains invalid characters (only alphanumeric and dash allowed)");
    }
    return clientId;
  }

  /** Invalidates all sessions and cleans up resources. */
  public void close() {
    tokenCache.invalidateAll();
    tokenCache.cleanUp();
  }
}
