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
package com.timecho.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationTemplateResp;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;

import com.timecho.iotdb.isession.ISession;
import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.read.common.RowRecord;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import static com.timecho.iotdb.session.SQLConstants.ALL_DB_SQL;
import static com.timecho.iotdb.session.SQLConstants.COMMA;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_DATABASE;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_FLUSH;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_PREFIX;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_SUFFIX;
import static com.timecho.iotdb.session.SQLConstants.SQL_TIME_GT;
import static com.timecho.iotdb.session.SQLConstants.SQL_TIME_LT;

public class Session extends org.apache.iotdb.session.Session implements ISession {
  public Session(String host, int rpcPort) {
    super(host, rpcPort);
  }

  public Session(String host, String rpcPort, String username, String password) {
    super(host, rpcPort, username, password);
  }

  public Session(String host, int rpcPort, String username, String password) {
    super(host, rpcPort, username, password);
  }

  public Session(String host, int rpcPort, String username, String password, int fetchSize) {
    super(host, rpcPort, username, password, fetchSize);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      long queryTimeoutInMs) {
    super(host, rpcPort, username, password, fetchSize, queryTimeoutInMs);
  }

  public Session(String host, int rpcPort, String username, String password, ZoneId zoneId) {
    super(host, rpcPort, username, password, zoneId);
  }

  public Session(
      String host, int rpcPort, String username, String password, boolean enableRedirection) {
    super(host, rpcPort, username, password, enableRedirection);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      boolean enableRedirection) {
    super(host, rpcPort, username, password, fetchSize, zoneId, enableRedirection);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    super(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        zoneId,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        enableRedirection,
        version);
  }

  public Session(List<String> nodeUrls, String username, String password) {
    super(nodeUrls, username, password);
  }

  public Session(List<String> nodeUrls, String username, String password, int fetchSize) {
    super(nodeUrls, username, password, fetchSize);
  }

  public Session(List<String> nodeUrls, String username, String password, ZoneId zoneId) {
    super(nodeUrls, username, password, zoneId);
  }

  public Session(
      List<String> nodeUrls,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    super(
        nodeUrls,
        username,
        password,
        fetchSize,
        zoneId,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        enableRedirection,
        version);
  }

  public Session(Builder builder) {
    super(builder);
  }

  @Override
  public WhiteListInfoResp getWhiteIpSet()
      throws IoTDBConnectionException, StatementExecutionException {
    WhiteListInfoResp resp;
    try {
      resp = defaultSessionConnection.getClient().getWhiteIpSet();
      RpcUtils.verifySuccess(resp.getStatus());
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().getWhiteIpSet();
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  @Override
  public void updateWhiteList(Set<String> ipSet)
      throws IoTDBConnectionException, StatementExecutionException {
    try {

      RpcUtils.verifySuccess(defaultSessionConnection.getClient().updateWhiteList(ipSet));
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          RpcUtils.verifySuccess(defaultSessionConnection.getClient().updateWhiteList(ipSet));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
  }

  @Override
  public LicenseInfoResp getLicenseInfo()
      throws StatementExecutionException, IoTDBConnectionException {
    LicenseInfoResp resp;
    try {
      resp = defaultSessionConnection.getClient().getLicenseInfo();

      RpcUtils.verifySuccess(resp.getStatus());
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().getLicenseInfo();
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql = buildQuerySql(databaseSet).append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql =
        buildQuerySql(databaseSet)
            .append(SQL_TIME_GT)
            .append(startTime)
            .append(SQL_TIME_LT)
            .append(endTime)
            .append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql =
        buildQuerySql(databaseSet)
            .append(SQL_TIME_GT)
            .append(startTime)
            .append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  @Override
  public TShowConfigurationTemplateResp showConfigurationTemplate()
      throws StatementExecutionException, IoTDBConnectionException {
    TShowConfigurationTemplateResp resp;
    try {
      resp = defaultSessionConnection.getClient().showConfigurationTemplate();
    } catch (Exception e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().showConfigurationTemplate();
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId)
      throws StatementExecutionException, IoTDBConnectionException {
    TShowConfigurationResp resp;
    try {
      resp = defaultSessionConnection.getClient().showConfiguration(nodeId);
    } catch (Exception e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().showConfiguration(nodeId);
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  private long getTotalPointsFromDataset(StringBuilder baseSql)
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet sessionDataSet = executeQueryStatement(baseSql.toString())) {
      if (sessionDataSet.hasNext()) {
        RowRecord next = sessionDataSet.next();
        if (next.hasNullField()) {
          return 0;
        }
        return (long) next.getFields().get(0).getDoubleV();
      }
    }
    return 0;
  }

  private static StringBuilder buildQuerySql(Set<String> databaseSet) {
    StringBuilder baseSql = new StringBuilder(ALL_DB_SQL);
    if (!CollectionUtils.isEmpty(databaseSet)) {
      baseSql = new StringBuilder(SPECIFY_DB_SQL_PREFIX);
      for (String database : databaseSet) {
        baseSql
            .append(SPECIFY_DB_SQL_DATABASE)
            .append(database)
            .append(SPECIFY_DB_SQL_FLUSH)
            .append(COMMA);
      }
      baseSql.deleteCharAt(baseSql.length() - 1);
    }
    return baseSql;
  }

  public static class Builder extends org.apache.iotdb.session.Session.Builder {

    @Override
    public Builder host(String host) {
      super.host(host);
      return this;
    }

    @Override
    public Builder port(int port) {
      super.port(port);
      return this;
    }

    @Override
    public Builder username(String username) {
      super.username(username);
      return this;
    }

    @Override
    public Builder password(String password) {
      super.password(password);
      return this;
    }

    @Override
    public Builder fetchSize(int fetchSize) {
      super.fetchSize(fetchSize);
      return this;
    }

    @Override
    public Builder zoneId(ZoneId zoneId) {
      super.zoneId(zoneId);
      return this;
    }

    @Override
    public Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
      super.thriftDefaultBufferSize(thriftDefaultBufferSize);
      return this;
    }

    @Override
    public Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
      super.thriftMaxFrameSize(thriftMaxFrameSize);
      return this;
    }

    @Override
    public Builder enableRedirection(boolean enableRedirection) {
      super.enableRedirection(enableRedirection);
      return this;
    }

    @Override
    public Builder enableRecordsAutoConvertTablet(boolean enableRecordsAutoConvertTablet) {
      super.enableRecordsAutoConvertTablet(enableRecordsAutoConvertTablet);
      return this;
    }

    @Override
    public Builder nodeUrls(List<String> nodeUrls) {
      super.nodeUrls(nodeUrls);
      return this;
    }

    @Override
    public Builder version(Version version) {
      super.version(version);
      return this;
    }

    @Override
    public Builder timeOut(long timeOut) {
      super.timeOut(timeOut);
      return this;
    }

    @Override
    public Builder enableAutoFetch(boolean enableAutoFetch) {
      super.enableAutoFetch(enableAutoFetch);
      return this;
    }

    @Override
    public Builder maxRetryCount(int maxRetryCount) {
      super.maxRetryCount(maxRetryCount);
      return this;
    }

    @Override
    public Builder retryIntervalInMs(long retryIntervalInMs) {
      super.retryIntervalInMs(retryIntervalInMs);
      return this;
    }

    @Override
    public Builder sqlDialect(String sqlDialect) {
      super.sqlDialect(sqlDialect);
      return this;
    }

    @Override
    public Builder database(String database) {
      super.database(database);
      return this;
    }

    @Override
    public Builder useSSL(boolean useSSL) {
      super.useSSL(useSSL);
      return this;
    }

    @Override
    public Builder trustStore(String keyStore) {
      super.trustStore(keyStore);
      return this;
    }

    @Override
    public Builder trustStorePwd(String keyStorePwd) {
      super.trustStorePwd(keyStorePwd);
      return this;
    }

    @Override
    public Builder sslProtocol(String sslProtocol) {
      super.sslProtocol(sslProtocol);
      return this;
    }

    @Override
    public Builder enableIoTDBRpcCompression(boolean enabled) {
      super.enableIoTDBRpcCompression(enabled);
      return this;
    }

    @Override
    public Builder enableThriftRpcCompression(boolean enabled) {
      super.enableThriftRpcCompression(enabled);
      return this;
    }

    @Override
    public com.timecho.iotdb.session.Session build() {
      if (nodeUrls != null
          && (!SessionConfig.DEFAULT_HOST.equals(host) || rpcPort != SessionConfig.DEFAULT_PORT)) {
        throw new IllegalArgumentException(
            "You should specify either nodeUrls or (host + rpcPort), but not both");
      }
      return new com.timecho.iotdb.session.Session(this);
    }
  }
}
