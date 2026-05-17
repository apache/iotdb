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

package com.timecho.iotdb.db.protocol.session;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class TimechoSessionManager extends SessionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimechoSessionManager.class);

  private static final int EXPIRATION_CHECK_INTERVAL_MS = 10_000;

  @SuppressWarnings("FieldCanBeLocal")
  private final ScheduledExecutorService scheduledExecutorService =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Expired-session-cleaner");

  public TimechoSessionManager() {
    super();
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduledExecutorService,
        this::cleanExpiredSessions,
        0,
        EXPIRATION_CHECK_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  private void cleanExpiredSessions() {
    int idleSessionTimeoutInMinutes =
        IoTDBDescriptor.getInstance().getConfig().getIdleSessionTimeoutInMinutes();
    if (idleSessionTimeoutInMinutes <= 0) {
      return;
    }
    Set<IClientSession> sessionsToRemove = new HashSet<>();

    for (IClientSession session : sessions.keySet()) {

      synchronized (session) {
        // avoid a session being cleaned during execution
        if (session.isRunning()) {
          continue;
        }

        long lastActiveTime = session.getLastActiveTime();
        long lastActiveTimeInMillis = CommonDateTimeUtils.convertIoTDBTimeToMillis(lastActiveTime);
        long idleSessionTimeoutInMillis = idleSessionTimeoutInMinutes * 60 * 1000L;

        if (System.currentTimeMillis() - lastActiveTimeInMillis > idleSessionTimeoutInMillis) {
          AUDIT_LOGGER.log(
              new AuditLogFields(
                  session.getUserId(),
                  session.getUsername(),
                  session.getClientAddress(),
                  AuditEventType.CONNECTION_EVICTED,
                  AuditLogOperation.CONTROL,
                  false),
              () ->
                  String.format(
                      "User %s (ID=%d) connection evicted due to idle timeout. "
                          + "No activity for %.1f minutes (exceeds threshold: %d minutes)",
                      session.getUsername(),
                      session.getUserId(),
                      System.currentTimeMillis() - lastActiveTimeInMillis / (60.0 * 1_000),
                      idleSessionTimeoutInMinutes));

          sessionsToRemove.add(session);
          // avoid the session being used
          session.setLogin(false);
          // release resources
          closeSession(session, Coordinator.getInstance()::cleanupQueryExecution, false);
          try {
            session.disconnect();
          } catch (IOException e) {
            LOGGER.error(
                TimechoServerMessages.CANNOT_DISCONNECT_EXPIRED_SESSION,
                session.getClientAddress(),
                e);
          }
        }
      }
    }

    sessionsToRemove.forEach(sessions::remove);
  }
}
