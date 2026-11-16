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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NOTICE: TableSessionWrapper is specific to the table model.
 *
 * <p>used for SessionPool.getSession need to do some other things like calling
 * cleanSessionAndMayThrowConnectionException in SessionPool while encountering connection exception
 * only need to putBack to SessionPool while closing.
 */
public class TableSessionWrapper implements ITableSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSessionWrapper.class);

  private Session session;

  private final SessionPool sessionPool;

  private final AtomicBoolean closed;

  protected TableSessionWrapper(Session session, SessionPool sessionPool) {
    this.session = session;
    this.sessionPool = sessionPool;
    this.closed = new AtomicBoolean(false);
  }

  @Override
  public void insert(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.insertRelationalTablet(tablet);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
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
  public void close() throws IoTDBConnectionException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    Session closingSession = session;
    session = null;
    try {
      if (!Objects.equals(closingSession.getSqlDialect(), sessionPool.sqlDialect)) {
        try {
          closingSession.executeNonQueryStatement("set sql_dialect=" + sessionPool.sqlDialect);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              SessionMessages
                  .LOG_FAILED_CHANGE_BACK_SQL_DIALECT_EXECUTING_SET_SQL_DIALECT_ARG_947F35E7,
              sessionPool.sqlDialect,
              e);
          sessionPool.cleanSessionAndMayThrowConnectionException(closingSession);
          return;
        }
      }

      if (!Objects.equals(closingSession.getDatabase(), sessionPool.database)
          && sessionPool.database != null) {
        try {
          closingSession.executeNonQueryStatement("use " + sessionPool.database);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              SessionMessages.LOG_FAILED_CHANGE_BACK_DATABASE_EXECUTING_USE_ARG_274541CA,
              sessionPool.database,
              e);
          sessionPool.cleanSessionAndMayThrowConnectionException(closingSession);
          return;
        }
      }

      sessionPool.putBack(closingSession);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(closingSession);
      throw e;
    }
  }
}
