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

import org.apache.iotdb.db.conf.IoTDBConstant.ClientVersion;

import java.time.ZoneId;
import java.util.TimeZone;

public abstract class IClientSession {

  /** id is just used for keep compatible with v0.13 */
  @Deprecated long id;

  ClientVersion clientVersion;

  ZoneId zoneId;

  // TODO: why some Statement Plans use timeZone while others use ZoneId?
  TimeZone timeZone;

  String username;

  boolean login = false;

  abstract String getClientAddress();

  abstract int getClientPort();

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

  @Deprecated
  public long getId() {
    return id;
  }

  @Deprecated
  public void setId(long id) {
    this.id = id;
  }

  public String toString() {
    return String.format("%d-%s:%d", getId(), getClientAddress(), getClientPort());
  }
}
