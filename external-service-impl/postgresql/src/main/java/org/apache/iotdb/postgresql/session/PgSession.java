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

package org.apache.iotdb.postgresql.session;

import org.apache.iotdb.db.protocol.session.ExternalServiceClientSession;

import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The state of one logged-in connection that the query layer needs: the database session, the
 * session parameters and the transaction status.
 */
public final class PgSession {

  private final ExternalServiceClientSession session;
  private final String user;
  private final Map<String, String> parameters = new LinkedHashMap<>();
  private final Map<String, String> startup = new LinkedHashMap<>();
  private volatile ZoneId startupZone;
  private volatile ZoneId zone;
  private volatile char transactionStatus = 'I';
  private volatile int processId;

  public PgSession(ExternalServiceClientSession session, String user, ZoneId zone) {
    this.session = session;
    this.user = user;
    this.zone = zone;
  }

  public ExternalServiceClientSession session() {
    return session;
  }

  public String user() {
    return user;
  }

  public ZoneId zone() {
    return zone;
  }

  public void setZone(ZoneId zone) {
    this.zone = zone;
    // the engine reads the zone from the database session (time literals, date_bin origins)
    session.setZoneId(zone);
  }

  /** The process id of BackendKeyData ({@code pg_backend_pid()}). */
  public int processId() {
    return processId;
  }

  public void setProcessId(int processId) {
    this.processId = processId;
  }

  /** 'I' idle, 'T' in a transaction block, 'E' in a failed transaction block. */
  public char transactionStatus() {
    return transactionStatus;
  }

  public void setTransactionStatus(char status) {
    this.transactionStatus = status;
  }

  /** Session parameters by lower-case name (set at startup or with SET). */
  public synchronized String parameter(String name) {
    return parameters.get(name);
  }

  public synchronized void setParameter(String name, String value) {
    parameters.put(name, value);
  }

  /** Records the current parameters as the values {@code RESET} returns to (end of startup). */
  public synchronized void markStartup() {
    startup.clear();
    startup.putAll(parameters);
    startupZone = zone;
  }

  /**
   * {@code RESET name}: the value at the end of startup, or none.
   *
   * @return the value now in effect, {@code null} if the parameter has none
   */
  public synchronized String reset(String name) {
    String value = startup.get(name);
    if (value == null) {
      parameters.remove(name);
    } else {
      parameters.put(name, value);
    }
    if (name.equals("timezone") && startupZone != null) {
      setZone(startupZone);
    }
    return value;
  }

  /** {@code RESET ALL}: every parameter back to its value at the end of startup. */
  public synchronized void resetAll() {
    parameters.clear();
    parameters.putAll(startup);
    if (startupZone != null) {
      setZone(startupZone);
    }
  }

  public synchronized Map<String, String> parameters() {
    return Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
  }
}
