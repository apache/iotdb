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
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.security.Identity;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;

public class SessionInfo {
  private final long sessionId;
  private final String userName;
  private final ZoneId zoneId;

  @Nullable private final String databaseName;

  private final IClientSession.SqlDialect sqlDialect;

  private ClientVersion version = ClientVersion.V_1_0;

  public SessionInfo(long sessionId, String userName, ZoneId zoneId) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.zoneId = zoneId;
    this.databaseName = null;
    this.sqlDialect = IClientSession.SqlDialect.TREE;
  }

  public SessionInfo(
      long sessionId,
      String userName,
      ZoneId zoneId,
      @Nullable String databaseName,
      IClientSession.SqlDialect sqlDialect) {
    this(sessionId, userName, zoneId, ClientVersion.V_1_0, databaseName, sqlDialect);
  }

  public SessionInfo(
      long sessionId,
      String userName,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      IClientSession.SqlDialect sqlDialect) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.zoneId = zoneId;
    this.version = version;
    this.databaseName = databaseName;
    this.sqlDialect = sqlDialect;
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

  public Identity getIdentity() {
    return new Identity(userName);
  }

  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  public IClientSession.SqlDialect getSqlDialect() {
    return sqlDialect;
  }

  public static SessionInfo deserializeFrom(final ByteBuffer buffer) {
    final long sessionId = ReadWriteIOUtils.readLong(buffer);
    final String userName = ReadWriteIOUtils.readString(buffer);
    final ZoneId zoneId = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(buffer)));
    final boolean hasDatabaseName = ReadWriteIOUtils.readBool(buffer);
    String databaseName = null;
    if (hasDatabaseName) {
      databaseName = ReadWriteIOUtils.readString(buffer);
    }
    final IClientSession.SqlDialect sqlDialect1 = IClientSession.SqlDialect.deserializeFrom(buffer);
    return new SessionInfo(sessionId, userName, zoneId, databaseName, sqlDialect1);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sessionId, stream);
    ReadWriteIOUtils.write(userName, stream);
    ReadWriteIOUtils.write(zoneId.getId(), stream);
    if (databaseName == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(databaseName, stream);
    }
    sqlDialect.serialize(stream);
  }

  public void serialize(final ByteBuffer buffer) {
    ReadWriteIOUtils.write(sessionId, buffer);
    ReadWriteIOUtils.write(userName, buffer);
    ReadWriteIOUtils.write(zoneId.getId(), buffer);
    if (databaseName == null) {
      ReadWriteIOUtils.write((byte) 0, buffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, buffer);
      ReadWriteIOUtils.write(databaseName, buffer);
    }
    sqlDialect.serialize(buffer);
  }
}
