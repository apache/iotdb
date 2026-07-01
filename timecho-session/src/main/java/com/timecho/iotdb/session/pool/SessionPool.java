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
package com.timecho.iotdb.session.pool;

import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationTemplateResp;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;
import org.apache.iotdb.session.pool.AbstractSessionPoolBuilder;

import com.timecho.iotdb.isession.ISession;
import com.timecho.iotdb.isession.ITableSession;
import com.timecho.iotdb.isession.pool.ISessionPool;
import com.timecho.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;

public class SessionPool extends org.apache.iotdb.session.pool.SessionPool implements ISessionPool {
  private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);

  public SessionPool(String host, int port, String user, String password, int maxSize) {
    super(host, port, user, password, maxSize);
  }

  public SessionPool(List<String> nodeUrls, String user, String password, int maxSize) {
    super(nodeUrls, user, password, maxSize);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, boolean enableCompression) {
    super(host, port, user, password, maxSize, enableCompression);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, boolean enableCompression) {
    super(nodeUrls, user, password, maxSize, enableCompression);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    super(host, port, user, password, maxSize, enableCompression, enableRedirection);
  }

  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    super(nodeUrls, user, password, maxSize, enableCompression, enableRedirection);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, ZoneId zoneId) {
    super(host, port, user, password, maxSize, zoneId);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, ZoneId zoneId) {
    super(nodeUrls, user, password, maxSize, zoneId);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    super(
        host,
        port,
        user,
        password,
        maxSize,
        fetchSize,
        waitToGetSessionTimeoutInMs,
        enableCompression,
        zoneId,
        enableRedirection,
        connectionTimeoutInMs,
        version,
        thriftDefaultBufferSize,
        thriftMaxFrameSize);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean useSSL,
      String trustStore,
      String trustStorePwd) {
    super(
        host,
        port,
        user,
        password,
        maxSize,
        fetchSize,
        waitToGetSessionTimeoutInMs,
        enableCompression,
        zoneId,
        enableRedirection,
        connectionTimeoutInMs,
        version,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        useSSL,
        trustStore,
        trustStorePwd);
  }

  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    super(
        nodeUrls,
        user,
        password,
        maxSize,
        fetchSize,
        waitToGetSessionTimeoutInMs,
        enableCompression,
        zoneId,
        enableRedirection,
        connectionTimeoutInMs,
        version,
        thriftDefaultBufferSize,
        thriftMaxFrameSize);
  }

  public SessionPool(AbstractSessionPoolBuilder builder) {
    super(builder);
  }

  public SessionPool(Builder builder) {
    super(builder);
  }

  @Override
  public WhiteListInfoResp getWhiteIpSet()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        WhiteListInfoResp whiteListInfoResp = session.getWhiteIpSet();
        occupy(session);
        return whiteListInfoResp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getWhiteIpSet failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  public void updateWhiteList(Set<String> ipSet)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        session.updateWhiteList(ipSet);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("updateWhiteList failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  @Override
  public LicenseInfoResp getLicenseInfo()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        LicenseInfoResp licenseInfoResp = session.getLicenseInfo();
        putBack(session);
        return licenseInfoResp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getLicenseInfo failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        long totalPoints = session.getTotalPoints(databaseSet);
        putBack(session);
        return totalPoints;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getTotalPoints failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return 0;
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        long totalPoints = session.getTotalPoints(databaseSet, startTime, endTime);
        putBack(session);
        return totalPoints;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getTotalPoints failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return 0;
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        long totalPoints = session.getTotalPoints(databaseSet, startTime);
        putBack(session);
        return totalPoints;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getTotalPoints failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return 0;
  }

  @Override
  public TShowConfigurationTemplateResp showConfigurationTemplate()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        TShowConfigurationTemplateResp result = session.showConfigurationTemplate();
        putBack(session);
        return result;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getTotalPoints failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        TShowConfigurationResp result = session.showConfiguration(nodeId);
        putBack(session);
        return result;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getTotalPoints failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  protected ISession constructNewSession() {
    Session session;
    if (nodeUrls == null) {
      // Construct custom Session
      session =
          new Session.Builder()
              .host(host)
              .port(port)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .enableRecordsAutoConvertTablet(enableRecordsAutoConvertTablet)
              .version(version)
              .useSSL(useSSL)
              .trustStore(trustStore)
              .trustStorePwd(trustStorePwd)
              .keyStore(keyStore)
              .keyStorePwd(keyStorePwd)
              .sslProtocol(sslProtocol)
              .maxRetryCount(maxRetryCount)
              .retryIntervalInMs(retryIntervalInMs)
              .sqlDialect(sqlDialect)
              .database(database)
              .timeOut(queryTimeoutInMs)
              .enableIoTDBRpcCompression(enableIoTDBRpcCompression)
              .enableThriftRpcCompression(enableThriftCompression)
              .build();
    } else {
      // Construct redirect-able Session
      session =
          new Session.Builder()
              .nodeUrls(nodeUrls)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .enableRecordsAutoConvertTablet(enableRecordsAutoConvertTablet)
              .version(version)
              .useSSL(useSSL)
              .trustStore(trustStore)
              .trustStorePwd(trustStorePwd)
              .keyStore(keyStore)
              .keyStorePwd(keyStorePwd)
              .sslProtocol(sslProtocol)
              .maxRetryCount(maxRetryCount)
              .retryIntervalInMs(retryIntervalInMs)
              .sqlDialect(sqlDialect)
              .database(database)
              .timeOut(queryTimeoutInMs)
              .enableIoTDBRpcCompression(enableIoTDBRpcCompression)
              .enableThriftRpcCompression(enableThriftCompression)
              .build();
    }
    session.setEnableQueryRedirection(enableQueryRedirection);
    return session;
  }

  @Override
  protected ITableSession getPooledTableSession() throws IoTDBConnectionException {
    return new TableSessionWrapper((Session) getSession(), this);
  }

  public static class Builder extends org.apache.iotdb.session.pool.SessionPool.Builder {

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
    public Builder nodeUrls(List<String> nodeUrls) {
      super.nodeUrls(nodeUrls);
      return this;
    }

    @Override
    public Builder maxSize(int maxSize) {
      super.maxSize(maxSize);
      return this;
    }

    @Override
    public Builder user(String user) {
      super.user(user);
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
    public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
      super.waitToGetSessionTimeoutInMs(waitToGetSessionTimeoutInMs);
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
    public Builder enableThriftRpcCompaction(boolean enableCompression) {
      super.enableThriftRpcCompaction(enableCompression);
      return this;
    }

    @Override
    public Builder enableIoTDBRpcCompression(boolean enableCompression) {
      super.enableIoTDBRpcCompression(enableCompression);
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
    public Builder connectionTimeoutInMs(int connectionTimeoutInMs) {
      super.connectionTimeoutInMs(connectionTimeoutInMs);
      return this;
    }

    @Override
    public Builder version(Version version) {
      super.version(version);
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
    public Builder queryTimeoutInMs(long queryTimeoutInMs) {
      super.queryTimeoutInMs(queryTimeoutInMs);
      return this;
    }

    protected Builder sqlDialect(String sqlDialect) {
      super.sqlDialect = sqlDialect;
      return this;
    }

    protected Builder database(String database) {
      super.database = database;
      return this;
    }

    @Override
    public SessionPool build() {
      return new SessionPool(this);
    }
  }
}
