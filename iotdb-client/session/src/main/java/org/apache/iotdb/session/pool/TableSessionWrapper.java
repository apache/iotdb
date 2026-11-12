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

  private boolean closed;

  protected TableSessionWrapper(Session session, SessionPool sessionPool) {
    this.session = session;
    this.sessionPool = sessionPool;
    this.closed = false;
  }

  @Override
  public void insert(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.insertRelationalTablet(tablet);
    } catch (IoTDBConnectionException e) {
      throw releaseOnConnectionFailure(e);
    }
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      throw releaseOnConnectionFailure(e);
    }
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql, timeoutInMs);
    } catch (IoTDBConnectionException e) {
      throw releaseOnConnectionFailure(e);
    }
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.executeNonQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      throw releaseOnConnectionFailure(e);
    }
  }

  @Override
  public void close() throws IoTDBConnectionException {
    final Session sessionToRelease;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      sessionToRelease = session;
      session = null;
    }

    if (sessionToRelease == null) {
      return;
    }

    if (sessionPool.isPoolClosed()) {
      sessionPool.releaseSession(sessionToRelease, false);
      return;
    }

    boolean reusable = true;
    try {
      if (!Objects.equals(sessionToRelease.getSqlDialect(), sessionPool.sqlDialect)) {
        try {
          sessionToRelease.executeNonQueryStatement("set sql_dialect=" + sessionPool.sqlDialect);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              SessionMessages
                  .LOG_FAILED_CHANGE_BACK_SQL_DIALECT_EXECUTING_SET_SQL_DIALECT_ARG_947F35E7,
              sessionPool.sqlDialect,
              e);
          reusable = false;
        }
      }

      if (reusable
          && !Objects.equals(sessionToRelease.getDatabase(), sessionPool.database)
          && sessionPool.database != null) {
        try {
          sessionToRelease.executeNonQueryStatement("use " + sessionPool.database);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              SessionMessages.LOG_FAILED_CHANGE_BACK_DATABASE_EXECUTING_USE_ARG_274541CA,
              sessionPool.database,
              e);
          reusable = false;
        }
      }
    } catch (IoTDBConnectionException | RuntimeException e) {
      sessionPool.releaseSession(sessionToRelease, false);
      throw e;
    }

    sessionPool.releaseSession(sessionToRelease, reusable);
  }

  private IoTDBConnectionException releaseOnConnectionFailure(IoTDBConnectionException exception) {
    final Session failedSession;
    synchronized (this) {
      closed = true;
      failedSession = session;
      session = null;
    }
    if (failedSession != null) {
      sessionPool.releaseSession(failedSession, false);
    }
    return exception;
  }
}
