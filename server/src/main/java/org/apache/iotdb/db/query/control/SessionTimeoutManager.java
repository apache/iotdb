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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SessionTimeoutManager {
  private static final long CLEANUP_PERIOD_MILLIS = 10 * 1000; // TODO: Use a more reasonable value
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTimeoutManager.class);

  private Map<Long, SessionInfo> idToSessionInfo;
  private ScheduledExecutorService executorService;

  private SessionTimeoutManager() {
    this.idToSessionInfo = new ConcurrentHashMap<>();

    this.executorService =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, "session-timeout-manager");
    executorService.scheduleAtFixedRate(
        () -> {
          LOGGER.info("cleaning up expired sessions");
          cleanup();
        },
        0,
        CLEANUP_PERIOD_MILLIS,
        TimeUnit.MILLISECONDS);
  }

  public void register(long id) {
    long timeout = IoTDBDescriptor.getInstance().getConfig().getSessionTimeoutThreshold();
    idToSessionInfo.put(id, new SessionInfo(timeout));
  }

  public void unregister(long id) {
    idToSessionInfo.remove(id);
  }

  public void refresh(long id) {
    LOGGER.debug(String.format("session-%d refreshed", id));
    idToSessionInfo.computeIfPresent(
        id,
        (k, v) -> {
          v.setLastActiveTime(System.currentTimeMillis());
          return v;
        });
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    idToSessionInfo.entrySet().stream()
        .filter(
            entry ->
                entry.getValue().getLastActiveTime() + entry.getValue().getMaxIdleTime()
                    < currentTime)
        .forEach(
            entry -> {
              if (SessionManager.getInstance().releaseSessionResource(entry.getKey())) {
                unregister(entry.getKey());
                LOGGER.debug(
                    String.format(
                        "session-%s timed out in %d ms",
                        entry.getKey(), currentTime - entry.getValue().getLastActiveTime()));
              }
            });
  }

  public static SessionTimeoutManager getInstance() {
    return SessionTimeoutManagerHelper.INSTANCE;
  }

  private static class SessionInfo {
    private long maxIdleTime;
    private long lastActiveTime;

    private SessionInfo(long maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      this.lastActiveTime = System.currentTimeMillis();
    }

    private long getLastActiveTime() {
      return lastActiveTime;
    }

    private void setLastActiveTime(long lastActiveTime) {
      this.lastActiveTime = lastActiveTime;
    }

    private long getMaxIdleTime() {
      return maxIdleTime;
    }
  }

  private static class SessionTimeoutManagerHelper {

    private static final SessionTimeoutManager INSTANCE = new SessionTimeoutManager();

    private SessionTimeoutManagerHelper() {}
  }
}
