/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Objects;

public class SessionInfo {
  private final long sessionId;
  private final String userName;
  private final ZoneId zoneId;
  private final String cliHostname;

  private ClientVersion version = ClientVersion.V_1_0;

  public SessionInfo(long sessionId, String userName, ZoneId zoneId, String cliHostname) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.zoneId = zoneId;
    this.cliHostname = cliHostname;
  }

  public SessionInfo(
      long sessionId, String userName, ZoneId zoneId, ClientVersion version, String cliHostname) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.zoneId = zoneId;
    this.version = version;
    this.cliHostname = cliHostname;
  }

  public long getSessionId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public ClientVersion getVersion() {
    return version;
  }

  public String getCliHostname() {
    return cliHostname;
  }

  public static SessionInfo deserializeFrom(ByteBuffer buffer) {
    long sessionId = ReadWriteIOUtils.readLong(buffer);
    String userName = ReadWriteIOUtils.readString(buffer);
    ZoneId zoneId = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(buffer)));
    String cliHostname = ReadWriteIOUtils.readString(buffer);
    return new SessionInfo(sessionId, userName, zoneId, cliHostname);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sessionId, stream);
    ReadWriteIOUtils.write(userName, stream);
    ReadWriteIOUtils.write(zoneId.getId(), stream);
    ReadWriteIOUtils.write(cliHostname, stream);
  }
}
