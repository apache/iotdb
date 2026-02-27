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

package org.apache.iotdb.flight;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Apache Arrow Flight SQL producer implementation for IoTDB. Handles SQL query execution via the
 * Arrow Flight SQL protocol, using the Table model SQL dialect.
 */
public class IoTDBFlightSqlProducer implements FlightSqlProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBFlightSqlProducer.class);
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final BufferAllocator allocator;
  private final FlightSqlSessionManager flightSessionManager;
  private final Coordinator coordinator = Coordinator.getInstance();
  private final SessionManager sessionManager = SessionManager.getInstance();
  private final SqlParser sqlParser = new SqlParser();
  private final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

  /** Stores query execution context by queryId for streaming results via getStream. */
  private final ConcurrentHashMap<Long, QueryContext> activeQueries = new ConcurrentHashMap<>();

  public IoTDBFlightSqlProducer(
      BufferAllocator allocator, FlightSqlSessionManager flightSessionManager) {
    this.allocator = allocator;
    this.flightSessionManager = flightSessionManager;
  }

  // ===================== Session Retrieval =====================

  /**
   * Retrieves the IClientSession for the current call. The auth2 framework sets the peer identity
   * to the Bearer token after handshake.
   */
  private IClientSession getSessionFromContext(CallContext context) {
    String peerIdentity = context.peerIdentity();
    if (peerIdentity == null || peerIdentity.isEmpty()) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Not authenticated").toRuntimeException();
    }
    try {
      return flightSessionManager.getSessionByToken(peerIdentity);
    } catch (SecurityException e) {
      throw CallStatus.UNAUTHENTICATED.withDescription(e.getMessage()).toRuntimeException();
    }
  }

  // ===================== SQL Query Execution =====================

  @Override
  public FlightInfo getFlightInfoStatement(
      FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    String sql = command.getQuery();
    LOGGER.warn("getFlightInfoStatement called with SQL: {}", sql);

    IClientSession session = getSessionFromContext(context);

    Long queryId = null;
    try {
      queryId = sessionManager.requestQueryId();

      // Parse the SQL statement
      Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), session);

      // Handle USE database statement: set database on session and return empty
      // result
      if (statement instanceof org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use) {
        org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use useStmt =
            (org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use) statement;
        String dbName = useStmt.getDatabaseId().getValue();
        session.setDatabaseName(dbName);
        LOGGER.info("Flight SQL session database set to: {}", dbName);
        Schema emptySchema = new Schema(Collections.emptyList());
        return new FlightInfo(emptySchema, descriptor, Collections.emptyList(), -1, -1);
      }

      // Execute via Coordinator (Table model)
      ExecutionResult result =
          coordinator.executeForTableModel(
              statement,
              sqlParser,
              session,
              queryId,
              sessionManager.getSessionInfo(session),
              sql,
              metadata,
              CONFIG.getQueryTimeoutThreshold(),
              true);

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        throw CallStatus.INTERNAL
            .withDescription("Query execution failed: " + result.status.getMessage())
            .toRuntimeException();
      }

      IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
      if (queryExecution == null) {
        throw CallStatus.INTERNAL
            .withDescription("Query execution not found after execution")
            .toRuntimeException();
      }

      DatasetHeader header = queryExecution.getDatasetHeader();
      Schema arrowSchema = TsBlockToArrowConverter.toArrowSchema(header);

      // Store the query context for later getStream calls
      activeQueries.put(queryId, new QueryContext(queryExecution, header, session));

      // Build ticket containing the queryId as a TicketStatementQuery protobuf.
      // FlightSqlClient.getStream() expects this format to route DoGet to
      // getStreamStatement().
      ByteString handle = ByteString.copyFromUtf8(Long.toString(queryId));
      FlightSql.TicketStatementQuery ticketQuery =
          FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
      Ticket ticket = new Ticket(com.google.protobuf.Any.pack(ticketQuery).toByteArray());
      FlightEndpoint endpoint = new FlightEndpoint(ticket);

      return new FlightInfo(arrowSchema, descriptor, Collections.singletonList(endpoint), -1, -1);

    } catch (Exception e) {
      // Cleanup on error
      if (queryId != null) {
        coordinator.cleanupQueryExecution(queryId);
        activeQueries.remove(queryId);
      }
      LOGGER.error("Error executing Flight SQL query: {}", sql, e);
      if (e instanceof org.apache.arrow.flight.FlightRuntimeException) {
        throw (org.apache.arrow.flight.FlightRuntimeException) e;
      }
      throw CallStatus.INTERNAL
          .withDescription("Query execution error: " + e.getMessage())
          .withCause(e)
          .toRuntimeException();
    }
  }

  @Override
  public SchemaResult getSchemaStatement(
      FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("getSchemaStatement not implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamStatement(
      FlightSql.TicketStatementQuery ticketQuery,
      CallContext context,
      ServerStreamListener listener) {
    ByteString handle = ticketQuery.getStatementHandle();
    long queryId = Long.parseLong(handle.toStringUtf8());
    LOGGER.warn("getStreamStatement called for queryId={}", queryId);
    try {
      streamQueryResults(queryId, listener);
    } catch (Throwable e) {
      LOGGER.error("getStreamStatement failed for queryId={}", queryId, e);
      listener.error(
          CallStatus.INTERNAL
              .withDescription("getStreamStatement error: " + e.getMessage())
              .toRuntimeException());
    }
  }

  /** Streams query results for a given queryId as Arrow VectorSchemaRoot batches. */
  private void streamQueryResults(long queryId, ServerStreamListener listener) {
    QueryContext ctx = activeQueries.get(queryId);
    if (ctx == null) {
      listener.error(
          CallStatus.NOT_FOUND
              .withDescription("Query not found for id: " + queryId)
              .toRuntimeException());
      return;
    }

    try (VectorSchemaRoot root =
        TsBlockToArrowConverter.createVectorSchemaRoot(ctx.header, allocator)) {
      listener.start(root);
      LOGGER.warn("streamQueryResults: listener started for queryId={}", queryId);

      int batchCount = 0;
      while (true) {
        LOGGER.warn("streamQueryResults: fetching batch {} for queryId={}", batchCount, queryId);
        Optional<TsBlock> optionalTsBlock = ctx.queryExecution.getBatchResult();
        if (!optionalTsBlock.isPresent()) {
          LOGGER.warn(
              "streamQueryResults: optionalTsBlock not present for queryId={}, breaking", queryId);
          break;
        }

        TsBlock tsBlock = optionalTsBlock.get();
        if (tsBlock.isEmpty()) {
          LOGGER.warn("streamQueryResults: tsBlock isEmpty for queryId={}, continuing", queryId);
          continue;
        }

        LOGGER.warn(
            "streamQueryResults: filling root with batch {} ({} rows)",
            batchCount,
            tsBlock.getPositionCount());
        TsBlockToArrowConverter.fillVectorSchemaRoot(root, tsBlock, ctx.header);
        listener.putNext();
        LOGGER.warn("streamQueryResults: putNext done for batch {}", batchCount);

        while (!listener.isReady() && !listener.isCancelled()) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        if (listener.isCancelled()) {
          LOGGER.warn("Flight stream cancelled by client for queryId={}", queryId);
          break;
        }
        batchCount++;
      }

      LOGGER.warn(
          "streamQueryResults: completing listener for queryId={}, total batches={}",
          queryId,
          batchCount);
      // Detach buffers from root so it's not freed while gRPC sends the last batch
      root.allocateNew();
      listener.completed();
    } catch (IoTDBException e) {
      LOGGER.error("Error streaming query results for queryId={}", queryId, e);
      listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
    } catch (Throwable e) {
      LOGGER.error("Unexpected error streaming query results for queryId={}", queryId, e);
      listener.error(
          CallStatus.INTERNAL
              .withDescription("Streaming error: " + e.getMessage())
              .toRuntimeException());
    } finally {
      coordinator.cleanupQueryExecution(queryId);
      activeQueries.remove(queryId);
    }
  }

  // ===================== Generic Flight Methods =====================

  @Override
  public void listFlights(
      CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    listener.onCompleted();
  }

  // ===================== Prepared Statements =====================

  @Override
  public void createPreparedStatement(
      FlightSql.ActionCreatePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    listener.onError(
        CallStatus.UNIMPLEMENTED
            .withDescription("Prepared statements are not yet supported")
            .toRuntimeException());
  }

  @Override
  public void closePreparedStatement(
      FlightSql.ActionClosePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    listener.onError(
        CallStatus.UNIMPLEMENTED
            .withDescription("Prepared statements are not yet supported")
            .toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      FlightSql.CommandPreparedStatementQuery command,
      CallContext context,
      FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Prepared statements are not yet supported")
        .toRuntimeException();
  }

  @Override
  public SchemaResult getSchemaPreparedStatement(
      FlightSql.CommandPreparedStatementQuery command,
      CallContext context,
      FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Prepared statements are not yet supported")
        .toRuntimeException();
  }

  @Override
  public void getStreamPreparedStatement(
      FlightSql.CommandPreparedStatementQuery command,
      CallContext context,
      ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Prepared statements are not yet supported")
        .toRuntimeException();
  }

  // ===================== DML/Update =====================

  @Override
  public Runnable acceptPutStatement(
      FlightSql.CommandStatementUpdate command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Statement updates are not yet supported")
        .toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
      FlightSql.CommandPreparedStatementUpdate command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Prepared statements are not yet supported")
        .toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
      FlightSql.CommandPreparedStatementQuery command,
      CallContext context,
      FlightStream flightStream,
      StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("Prepared statements are not yet supported")
        .toRuntimeException();
  }

  // ===================== Catalog/Schema/Table Metadata =====================

  @Override
  public FlightInfo getFlightInfoSqlInfo(
      FlightSql.CommandGetSqlInfo command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetSqlInfo not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(
      FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetSqlInfo not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(
      FlightSql.CommandGetXdbcTypeInfo command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTypeInfo not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamTypeInfo(
      FlightSql.CommandGetXdbcTypeInfo command,
      CallContext context,
      ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTypeInfo not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(
      FlightSql.CommandGetCatalogs command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetCatalogs not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetCatalogs not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSchemas(
      FlightSql.CommandGetDbSchemas command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetSchemas not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamSchemas(
      FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetSchemas not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTables(
      FlightSql.CommandGetTables command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTables not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamTables(
      FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTables not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(
      FlightSql.CommandGetTableTypes command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTableTypes not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetTableTypes not yet implemented")
        .toRuntimeException();
  }

  // ===================== Keys =====================

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
      FlightSql.CommandGetPrimaryKeys command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetPrimaryKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(
      FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetPrimaryKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
      FlightSql.CommandGetExportedKeys command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetExportedKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamExportedKeys(
      FlightSql.CommandGetExportedKeys command,
      CallContext context,
      ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetExportedKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
      FlightSql.CommandGetImportedKeys command, CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetImportedKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamImportedKeys(
      FlightSql.CommandGetImportedKeys command,
      CallContext context,
      ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetImportedKeys not yet implemented")
        .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(
      FlightSql.CommandGetCrossReference command,
      CallContext context,
      FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetCrossReference not yet implemented")
        .toRuntimeException();
  }

  @Override
  public void getStreamCrossReference(
      FlightSql.CommandGetCrossReference command,
      CallContext context,
      ServerStreamListener listener) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("GetCrossReference not yet implemented")
        .toRuntimeException();
  }

  // ===================== Lifecycle =====================

  @Override
  public void close() throws Exception {
    // Clean up all active queries
    for (Long queryId : activeQueries.keySet()) {
      try {
        coordinator.cleanupQueryExecution(queryId);
      } catch (Exception e) {
        LOGGER.warn("Error cleaning up query {} during shutdown", queryId, e);
      }
    }
    activeQueries.clear();
  }

  // ===================== Inner Classes =====================

  /** Holds query execution context for streaming results. */
  private static class QueryContext {

    final IQueryExecution queryExecution;
    final DatasetHeader header;
    final IClientSession session;

    QueryContext(IQueryExecution queryExecution, DatasetHeader header, IClientSession session) {
      this.queryExecution = queryExecution;
      this.header = header;
      this.session = session;
    }
  }
}
