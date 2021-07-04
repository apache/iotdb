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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SessionTimeoutManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTimeoutManager.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private long sessionTimeout = 0;
  private Map<Long, Long> sessionIdToLastActiveTime;
  private ScheduledExecutorService executorService;
  private ScheduledFuture future;

  private SessionTimeoutManager() {
    this.sessionIdToLastActiveTime = new ConcurrentHashMap<>();
    this.executorService =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "session-timeout-manager");
  }

  public void register(long id) {
    reschedule();
    if (sessionTimeout == 0) {
      return;
    }

    sessionIdToLastActiveTime.put(id, System.currentTimeMillis());
  }

  public void unregister(long id) {
    if (sessionTimeout == 0) {
      return;
    }

    sessionIdToLastActiveTime.remove(id);
  }

  public void refresh(long id) {
    if (sessionTimeout == 0) {
      return;
    }

    sessionIdToLastActiveTime.computeIfPresent(id, (k, v) -> System.currentTimeMillis());
  }

  private void reschedule() {
    long newSessionTimeout = CONFIG.getSessionTimeoutThreshold();
    if (newSessionTimeout == sessionTimeout) {
      return;
    }

    sessionTimeout = newSessionTimeout;

    if (future != null) {
      future.cancel(false);
    }

    if (newSessionTimeout != 0) {
      future =
          executorService.scheduleAtFixedRate(
              () -> {
                LOGGER.info("cleaning up expired sessions");
                cleanup();
              },
              0,
              Math.max(2000, CONFIG.getSessionTimeoutThreshold()),
              TimeUnit.MILLISECONDS);
    }
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    sessionIdToLastActiveTime.entrySet().stream()
        .filter(entry -> entry.getValue() + sessionTimeout < currentTime)
        .forEach(
            entry -> {
              if (SessionManager.getInstance().releaseSessionResource(entry.getKey())) {
                unregister(entry.getKey());
                LOGGER.debug(
                    String.format(
                        "session-%s timed out in %d ms",
                        entry.getKey(), currentTime - entry.getValue()));
              }
            });
  }

  public static SessionTimeoutManager getInstance() {
    return SessionTimeoutManagerHelper.INSTANCE;
  }

  private static class SessionTimeoutManagerHelper {

    private static final SessionTimeoutManager INSTANCE = new SessionTimeoutManager();

    private SessionTimeoutManagerHelper() {}
  }
}
