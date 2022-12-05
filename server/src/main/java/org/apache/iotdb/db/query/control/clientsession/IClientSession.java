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
package org.apache.iotdb.db.query.control.clientsession;

import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionType;

import java.time.ZoneId;
import java.util.Set;
import java.util.TimeZone;

public abstract class IClientSession {

  private long id;

  private ClientVersion clientVersion;

  private ZoneId zoneId;

  // TODO: why some Statement Plans use timeZone while others use ZoneId?
  private TimeZone timeZone;

  private String username;

  private boolean login = false;

  private long logInTime;

  public abstract String getClientAddress();

  abstract int getClientPort();

  abstract TSConnectionType getConnectionType();

  /** ip:port for thrift-based service and client id for mqtt-based service */
  abstract String getConnectionId();

  public void setClientVersion(ClientVersion clientVersion) {
    this.clientVersion = clientVersion;
  }

  public ClientVersion getClientVersion() {
    return this.clientVersion;
  }

  public ZoneId getZoneId() {
    return this.zoneId;
  }

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
    this.timeZone = TimeZone.getTimeZone(zoneId);
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
    this.zoneId = timeZone.toZoneId();
  }

  public String getUsername() {
    return this.username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public boolean isLogin() {
    return login;
  }

  public void setLogin(boolean login) {
    this.login = login;
  }

  public void setLogInTime(long logInTime) {
    this.logInTime = logInTime;
  }

  public long getLogInTime() {
    return logInTime;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String toString() {
    return String.format("%d-%s:%s", getId(), getUsername(), getConnectionId());
  }

  public TSConnectionInfo convertToTSConnectionInfo() {
    return new TSConnectionInfo(
        getUsername(), getLogInTime(), getConnectionId(), getConnectionType());
  }

  /**
   * statementIds that this client opens.<br>
   * For JDBC clients, each Statement instance has a statement id.<br>
   * For an IoTDBSession connection, each connection has a statement id.<br>
   * mqtt clients have no statement id.
   */
  public abstract Iterable<Long> getStatementIds();

  public abstract void addStatementId(long statementId);

  public abstract Set<Long> removeStatementId(long statementId);

  public abstract void addQueryId(Long statementId, long queryId);

  public abstract void removeQueryId(Long statementId, Long queryId);
}
