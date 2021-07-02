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
  private static final long SESSION_TIMEOUT =
      IoTDBDescriptor.getInstance().getConfig().getSessionTimeoutThreshold();
  private static final long CLEANUP_PERIOD_MILLIS = Math.max(5000, SESSION_TIMEOUT / 5);
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionTimeoutManager.class);

  private Map<Long, Long> idToLastActiveTime;
  private ScheduledExecutorService executorService;

  private SessionTimeoutManager() {
    if (SESSION_TIMEOUT == 0) return;

    this.idToLastActiveTime = new ConcurrentHashMap<>();

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
    if (SESSION_TIMEOUT == 0) return;

    idToLastActiveTime.put(id, System.currentTimeMillis());
  }

  public void unregister(long id) {
    if (SESSION_TIMEOUT == 0) return;

    idToLastActiveTime.remove(id);
  }

  public void refresh(long id) {
    if (SESSION_TIMEOUT == 0) return;

    LOGGER.debug(String.format("session-%d refreshed", id));
    idToLastActiveTime.computeIfPresent(id, (k, v) -> System.currentTimeMillis());
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    idToLastActiveTime.entrySet().stream()
        .filter(entry -> entry.getValue() + SESSION_TIMEOUT < currentTime)
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
