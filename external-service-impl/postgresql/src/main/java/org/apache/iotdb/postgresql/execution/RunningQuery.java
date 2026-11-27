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

package org.apache.iotdb.postgresql.execution;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExecuteImmediate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.postgresql.protocol.PgErrors;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * One table-model query started through the engine: parsed with the table dialect, run with the
 * engine's query timeout as a read-only user query, and always cleaned up.
 */
public final class RunningQuery implements AutoCloseable {

  private final long queryId;
  private final IQueryExecution execution;
  private final List<String> names;
  private final List<TSDataType> types;
  private final int[] blockIndex;
  private volatile boolean closed;

  private RunningQuery(long queryId, IQueryExecution execution) {
    this.queryId = queryId;
    this.execution = execution;
    this.names = new ArrayList<>(execution.getDatasetHeader().getRespColumns());
    this.types = new ArrayList<>(execution.getDatasetHeader().getRespDataTypes());
    List<Integer> mapping = execution.getDatasetHeader().getColumnIndex2TsBlockColumnIndexList();
    this.blockIndex = new int[names.size()];
    for (int i = 0; i < blockIndex.length; i++) {
      blockIndex[i] = mapping == null ? i : mapping.get(i);
    }
  }

  /**
   * Starts a query.
   *
   * @param sql table-model SQL (from the translator)
   * @param parameters the values of its {@code ?} in order; empty for none
   */
  public static RunningQuery start(IClientSession client, String sql, List<Literal> parameters)
      throws PgException {
    return start(client, sql, parameters, java.time.Duration.ZERO);
  }

  /**
   * Starts a query with the session's {@code statement_timeout}.
   *
   * @param statementTimeout limits the engine's query timeout; zero for none
   */
  public static RunningQuery start(
      IClientSession client,
      String sql,
      List<Literal> parameters,
      java.time.Duration statementTimeout)
      throws PgException {
    SessionManager sessions = SessionManager.getInstance();
    SqlParser parser = new SqlParser();
    IClientSession previous = sessions.getCurrSession();
    sessions.setCurrSession(client);
    Long queryId = null;
    boolean started = false;
    try {
      Statement statement =
          parameters.isEmpty()
              ? parser.createStatement(sql, client.getZoneId(), client)
              : new ExecuteImmediate(new StringLiteral(sql), parameters);
      queryId = sessions.requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTableModel(
                  statement,
                  parser,
                  client,
                  queryId,
                  // catalog queries of BI tools run first (and despite a full operator pool)
                  sessions.getSessionInfo(client).setMetadataQueriesFirst(true),
                  sql,
                  LocalExecutionPlanner.getInstance().metadata,
                  org.apache.iotdb.postgresql.session.StatementTimeout.engineTimeoutMillis(
                      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                      statementTimeout),
                  true,
                  false,
                  true);
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        throw PgErrors.of(result.status);
      }
      IQueryExecution execution = Coordinator.getInstance().getQueryExecution(queryId);
      if (execution == null) {
        throw new PgException(SqlState.INTERNAL_ERROR, "the statement did not start a query");
      }
      started = true;
      return new RunningQuery(queryId, execution);
    } catch (PgException e) {
      throw e;
    } catch (RuntimeException e) {
      throw PgErrors.of(errorCode(e), e);
    } finally {
      if (!started && queryId != null) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
      // the session stays registered (SHOW SESSIONS); only this thread forgets it
      sessions.restoreSession(previous, null);
    }
  }

  private static int errorCode(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof IoTDBException) {
        return ((IoTDBException) t).getErrorCode();
      }
      if (t instanceof org.apache.iotdb.commons.exception.IoTDBRuntimeException) {
        return ((org.apache.iotdb.commons.exception.IoTDBRuntimeException) t).getErrorCode();
      }
    }
    if (error instanceof SemanticException) {
      return TSStatusCode.SEMANTIC_ERROR.getStatusCode();
    }
    if (error instanceof ParsingException) {
      return TSStatusCode.SQL_PARSE_ERROR.getStatusCode();
    }
    return TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
  }

  /** Column names in result order. */
  public List<String> names() {
    return Collections.unmodifiableList(names);
  }

  /** Column types in result order. */
  public List<TSDataType> types() {
    return Collections.unmodifiableList(types);
  }

  /** The block column of result column {@code i}. */
  public int blockIndex(int i) {
    return blockIndex[i];
  }

  /** The next non-empty block, or empty at the end. */
  public Optional<TsBlock> next() throws PgException {
    try {
      while (execution.hasNextResult()) {
        Optional<TsBlock> block = execution.getBatchResult();
        if (!block.isPresent()) {
          return Optional.empty();
        }
        if (!block.get().isEmpty()) {
          return block;
        }
      }
      return Optional.empty();
    } catch (IoTDBException e) {
      throw PgErrors.of(e.getErrorCode(), e);
    } catch (RuntimeException e) {
      if (closed) {
        throw new PgException(SqlState.QUERY_CANCELED, "canceling statement due to user request");
      }
      throw PgErrors.of(errorCode(e), e);
    }
  }

  /** Stops the query (CancelRequest, write timeout); the reader then gets 57014. */
  public void cancel() {
    closed = true;
    execution.cancel();
  }

  @Override
  public void close() {
    closed = true;
    Coordinator.getInstance().cleanupQueryExecution(queryId);
  }
}
