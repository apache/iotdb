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

package org.apache.iotdb.commons.queryengine.common;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.queryengine.plan.relational.security.Identity;
import org.apache.iotdb.commons.utils.TestOnly;

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
  private final UserEntity userEntity;
  private final ZoneId zoneId;

  @Nullable private final String databaseName;

  private final SqlDialect sqlDialect;

  private ClientVersion version = ClientVersion.V_1_0;

  // Only used during statement analysis on the coordinator. Analysis is complete before the
  // SessionInfo is sent to execution nodes, so this field does not need serialization.
  private final transient boolean enableAutoCreateSchema;

  // These two flags are orthogonal and must not be used as substitutes for each other.
  //
  // True when the operation is executed as the database itself rather than as a regular client.
  // It is used by analysis/security checks for dedicated internal writes and system-database
  // access. Typical producers are database-owned maintenance/load paths and trusted internal RPC
  // operations that explicitly mark their analysis context as database-internal. It does not
  // indicate which RPC endpoint or service name was used.
  //
  // Like enableAutoCreateSchema, this state is only consulted during analysis and is not
  // serialized.
  private transient boolean internalDatabaseSession;

  // True when the session was opened through the multiplexed IClientRPCService on the DataNode
  // internal endpoint, for example by SessionManager.loginInternal(). It identifies the RPC
  // endpoint family and is used to select internal redirect endpoints and apply internal-only
  // transport behavior. It does not grant database-internal privileges by itself.
  //
  // A normal client session on the internal endpoint therefore has internalRpcSession=true while
  // internalDatabaseSession remains false unless its execution context explicitly represents a
  // database-internal operation. This transient state is not serialized.
  private transient boolean internalRpcSession;

  public SessionInfo(long sessionId, UserEntity userEntity, ZoneId zoneId) {
    this.sessionId = sessionId;
    this.userEntity = userEntity;
    this.zoneId = zoneId;
    this.databaseName = null;
    this.sqlDialect = SqlDialect.TREE;
    this.enableAutoCreateSchema = true;
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      @Nullable String databaseName,
      SqlDialect sqlDialect) {
    this(sessionId, userEntity, zoneId, ClientVersion.V_1_0, databaseName, sqlDialect, true);
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      SqlDialect sqlDialect) {
    this(sessionId, userEntity, zoneId, version, databaseName, sqlDialect, true);
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      SqlDialect sqlDialect,
      boolean enableAutoCreateSchema) {
    this.sessionId = sessionId;
    this.userEntity = userEntity;
    this.zoneId = zoneId;
    this.version = version;
    this.databaseName = databaseName;
    this.sqlDialect = sqlDialect;
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public SessionInfo(
      long sessionId,
      UserEntity userEntity,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      SqlDialect sqlDialect,
      boolean enableAutoCreateSchema,
      boolean internalDatabaseSession,
      boolean internalRpcSession) {
    this(sessionId, userEntity, zoneId, version, databaseName, sqlDialect, enableAutoCreateSchema);
    this.internalDatabaseSession = internalDatabaseSession;
    this.internalRpcSession = internalRpcSession;
  }

  public SessionInfo(
      final SessionInfo sessionInfo, final ZoneId zoneId, final SqlDialect sqlDialect) {
    this(
        sessionInfo.sessionId,
        sessionInfo.userEntity,
        zoneId,
        sessionInfo.version,
        sessionInfo.databaseName,
        sqlDialect,
        sessionInfo.enableAutoCreateSchema,
        sessionInfo.internalDatabaseSession,
        sessionInfo.internalRpcSession);
  }

  @TestOnly
  public SessionInfo(long sessionId, String username, ZoneId zoneId) {
    this(sessionId, new UserEntity(-1, username, ""), zoneId, null, SqlDialect.TREE);
  }

  @TestOnly
  public SessionInfo(
      long sessionId,
      String username,
      ZoneId zoneId,
      @Nullable String databaseName,
      SqlDialect sqlDialect) {
    this(sessionId, new UserEntity(-1, username, ""), zoneId, databaseName, sqlDialect);
  }

  @TestOnly
  public SessionInfo(
      long sessionId,
      String username,
      ZoneId zoneId,
      ClientVersion version,
      @Nullable String databaseName,
      SqlDialect sqlDialect) {
    this(sessionId, new UserEntity(-1, username, ""), zoneId, version, databaseName, sqlDialect);
  }

  public long getSessionId() {
    return sessionId;
  }

  public UserEntity getUserEntity() {
    return userEntity;
  }

  public long getUserId() {
    return userEntity.getUserId();
  }

  public String getUserName() {
    return userEntity.getUsername();
  }

  public String getCliHostname() {
    return userEntity.getCliHostname();
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public ClientVersion getVersion() {
    return version;
  }

  public Identity getIdentity() {
    return new Identity(userEntity.getUsername());
  }

  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  public SqlDialect getSqlDialect() {
    return sqlDialect;
  }

  public boolean isEnableAutoCreateSchema() {
    return enableAutoCreateSchema;
  }

  public boolean isInternalDatabaseSession() {
    return internalDatabaseSession;
  }

  public SessionInfo setInternalDatabaseSession(final boolean internalDatabaseSession) {
    this.internalDatabaseSession = internalDatabaseSession;
    return this;
  }

  public boolean isInternalRpcSession() {
    return internalRpcSession;
  }

  public SessionInfo setInternalRpcSession(final boolean internalRpcSession) {
    this.internalRpcSession = internalRpcSession;
    return this;
  }

  public static SessionInfo deserializeFrom(final ByteBuffer buffer) {
    final long sessionId = ReadWriteIOUtils.readLong(buffer);
    final long userId = ReadWriteIOUtils.readLong(buffer);
    final String userName = ReadWriteIOUtils.readString(buffer);
    final String cliHostname = ReadWriteIOUtils.readString(buffer);
    final ZoneId zoneId = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(buffer)));
    final boolean hasDatabaseName = ReadWriteIOUtils.readBool(buffer);
    String databaseName = null;
    if (hasDatabaseName) {
      databaseName = ReadWriteIOUtils.readString(buffer);
    }
    final SqlDialect sqlDialect1 = SqlDialect.deserializeFrom(buffer);
    return new SessionInfo(
        sessionId,
        new UserEntity(userId, userName, cliHostname),
        zoneId,
        databaseName,
        sqlDialect1);
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sessionId, stream);
    ReadWriteIOUtils.write(userEntity.getUserId(), stream);
    ReadWriteIOUtils.write(userEntity.getUsername(), stream);
    ReadWriteIOUtils.write(userEntity.getCliHostname(), stream);
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
    ReadWriteIOUtils.write(userEntity.getUserId(), buffer);
    ReadWriteIOUtils.write(userEntity.getUsername(), buffer);
    ReadWriteIOUtils.write(userEntity.getCliHostname(), buffer);
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
