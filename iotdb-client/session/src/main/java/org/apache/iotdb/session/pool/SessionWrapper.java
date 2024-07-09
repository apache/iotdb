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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.isession.IPooledSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.session.Session;

import org.apache.thrift.TException;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * used for SessionPool.getSession need to do some other things like calling
 * cleanSessionAndMayThrowConnectionException in SessionPool while encountering connection exception
 * only need to putBack to SessionPool while closing
 */
public class SessionWrapper implements IPooledSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionWrapper.class);

  private Session session;

  private final SessionPool sessionPool;

  private final AtomicBoolean closed;

  public SessionWrapper(Session session, SessionPool sessionPool) {
    this.session = session;
    this.sessionPool = sessionPool;
    this.closed = new AtomicBoolean(false);
  }

  @Override
  public Version getVersion() {
    return session.getVersion();
  }

  @Override
  public int getFetchSize() {
    return session.getFetchSize();
  }

  @Override
  public void close() throws IoTDBConnectionException {
    if (closed.compareAndSet(false, true)) {
      if (!Objects.equals(session.getDatabase(), sessionPool.database)
          && sessionPool.database != null) {
        try {
          session.executeNonQueryStatement("use " + sessionPool.database);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              "Failed to change back database by executing: use " + sessionPool.database, e);
        }
      }
      sessionPool.putBack(session);
      session = null;
    }
  }

  @Override
  public String getTimeZone() {
    return session.getTimeZone();
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql, timeoutInMs);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.executeNonQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public String getTimestampPrecision() throws TException {
    try {
      return session.getTimestampPrecision();
    } catch (TException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.insertTablet(tablet);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public boolean isEnableQueryRedirection() {
    return session.isEnableQueryRedirection();
  }

  @Override
  public boolean isEnableRedirection() {
    return session.isEnableRedirection();
  }

  @Override
  public TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      return session.getBackupConfiguration();
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {
    try {
      return session.fetchAllConnections();
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public long getQueryTimeout() {
    return session.getQueryTimeout();
  }
}
