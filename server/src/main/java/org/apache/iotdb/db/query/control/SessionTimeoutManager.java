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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.clientsession.ClientSession;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.service.basic.ServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SessionTimeoutManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTimeoutManager.class);
  private static final long MINIMUM_CLEANUP_PERIOD = 2000;
  private static final long SESSION_TIMEOUT =
      IoTDBDescriptor.getInstance().getConfig().getSessionTimeoutThreshold();

  private Map<IClientSession, Long> sessionToLastActiveTime;
  private ScheduledExecutorService executorService;

  private SessionTimeoutManager() {
    if (SESSION_TIMEOUT == 0) {
      return;
    }

    this.sessionToLastActiveTime = new ConcurrentHashMap<>();
    this.executorService =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "session-timeout-manager");

    executorService.scheduleAtFixedRate(
        () -> {
          LOGGER.info("cleaning up expired sessions");
          cleanup();
        },
        0,
        Math.max(MINIMUM_CLEANUP_PERIOD, SESSION_TIMEOUT / 5),
        TimeUnit.MILLISECONDS);
  }

  public void register(IClientSession session) {
    if (SESSION_TIMEOUT == 0) {
      return;
    }

    sessionToLastActiveTime.put(session, System.currentTimeMillis());
  }

  /**
   * unregister the session and release all its query resources.
   *
   * @param session
   * @return true if removing successfully, false otherwise (e.g., the session does not exist)
   */
  public boolean unregister(IClientSession session) {
    if (SESSION_TIMEOUT == 0) {
      return ServiceProvider.SESSION_MANAGER.releaseSessionResource(session);
    }

    if (ServiceProvider.SESSION_MANAGER.releaseSessionResource(session)) {
      return sessionToLastActiveTime.remove(session) != null;
    }

    return false;
  }

  public void refresh(IClientSession session) {
    if (SESSION_TIMEOUT == 0) {
      return;
    }

    sessionToLastActiveTime.computeIfPresent(session, (k, v) -> System.currentTimeMillis());
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    sessionToLastActiveTime.entrySet().stream()
        .filter(entry -> entry.getValue() + SESSION_TIMEOUT < currentTime)
        .forEach(
            entry -> {
              if (unregister(entry.getKey())) {
                LOGGER.debug(
                    String.format(
                        "session-%s timed out in %d ms",
                        entry.getKey(), currentTime - entry.getValue()));
                // close the socket.
                // currently, we only focus on RPC service.
                // TODO do we need to consider MQTT ClientSession?
                if (entry.getKey() instanceof ClientSession) {
                  ((ClientSession) entry.getKey()).shutdownStream();
                }
              }
            });
  }

  public static SessionTimeoutManager getInstance() {
    return SessionTimeoutManagerHelper.INSTANCE;
  }

  public boolean isSessionAlive(IClientSession session) {
    if (SESSION_TIMEOUT == 0) {
      return true;
    }
    return sessionToLastActiveTime.containsKey(session);
  }

  private static class SessionTimeoutManagerHelper {

    private static final SessionTimeoutManager INSTANCE = new SessionTimeoutManager();

    private SessionTimeoutManagerHelper() {}
  }
}
