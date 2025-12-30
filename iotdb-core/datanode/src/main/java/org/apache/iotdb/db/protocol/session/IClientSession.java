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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.common.ConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Set;
import java.util.TimeZone;

public abstract class IClientSession {

  private long id;

  private ClientVersion clientVersion;

  private ZoneId zoneId;

  private TimeZone timeZone;

  private long userId;

  private String username;

  private boolean login = false;

  private long logInTime;

  private SqlDialect sqlDialect = SqlDialect.TREE;

  @Nullable private String databaseName;

  private long lastActiveTime = CommonDateTimeUtils.currentTime();

  public abstract String getClientAddress();

  public abstract int getClientPort();

  abstract TSConnectionType getConnectionType();

  /** ip:port for thrift-based service and client id for mqtt-based service. */
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

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
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

  public ConnectionInfo convertToConnectionInfo() {
    return new ConnectionInfo(
        getUserId(), getUsername(), getId(), getLastActiveTime(), getClientAddress());
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

  // statementId could be null
  public abstract void removeQueryId(Long statementId, Long queryId);

  public SqlDialect getSqlDialect() {
    return sqlDialect;
  }

  public void setSqlDialect(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  public void setSqlDialectAndClean(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
    // clean database to avoid misuse of it between different SqlDialect
    this.databaseName = null;
  }

  @Nullable
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(@Nullable String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Add a prepared statement to this session.
   *
   * @param statementName the name of the prepared statement
   * @param info the prepared statement information
   */
  public abstract void addPreparedStatement(String statementName, PreparedStatementInfo info);

  /**
   * Remove a prepared statement from this session.
   *
   * @param statementName the name of the prepared statement
   * @return the removed prepared statement info, or null if not found
   */
  public abstract PreparedStatementInfo removePreparedStatement(String statementName);

  /**
   * Get a prepared statement from this session.
   *
   * @param statementName the name of the prepared statement
   * @return the prepared statement info, or null if not found
   */
  public abstract PreparedStatementInfo getPreparedStatement(String statementName);

  /**
   * Get all prepared statement names in this session.
   *
   * @return set of prepared statement names
   */
  public abstract Set<String> getPreparedStatementNames();

  public long getLastActiveTime() {
    return lastActiveTime;
  }

  public void setLastActiveTime(long lastActiveTime) {
    this.lastActiveTime = lastActiveTime;
  }

  public enum SqlDialect {
    TREE((byte) 0),
    TABLE((byte) 1);

    private final byte dialect;

    SqlDialect(byte dialect) {
      this.dialect = dialect;
    }

    public byte getDialect() {
      return dialect;
    }

    public void serialize(final DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(dialect, stream);
    }

    public void serialize(final ByteBuffer buffer) {
      ReadWriteIOUtils.write(dialect, buffer);
    }

    public static SqlDialect deserializeFrom(final ByteBuffer buffer) {
      byte b = ReadWriteIOUtils.readByte(buffer);
      switch (b) {
        case 0:
          return TREE;
        case 1:
          return TABLE;
        default:
          throw new IllegalArgumentException(String.format("Unknown sql dialect: %s", b));
      }
    }
  }
}
