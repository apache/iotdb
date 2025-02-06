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

package org.apache.iotdb.db.pipe.connector.protocol.writeback;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.apache.iotdb.db.exception.metadata.DatabaseNotSetException.DATABASE_NOT_SET;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.getRootCause;

@TreeModel
@TableModel
public class WriteBackConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackConnector.class);

  // Simulate the behavior of the client-to-server communication
  // for correctly handling data insertion in IoTDBReceiverAgent#receive method
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private IClientSession session;

  private static final String TREE_MODEL_DATABASE_NAME_IDENTIFIER = null;

  private static final SqlParser RELATIONAL_SQL_PARSER = new SqlParser();

  private static final Set<String> ALREADY_CREATED_DATABASES = ConcurrentHashMap.newKeySet();

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    session =
        new InternalClientSession(
            String.format(
                "%s_%s_%s_%s",
                WriteBackConnector.class.getSimpleName(),
                environment.getPipeName(),
                environment.getCreationTime(),
                environment.getRegionId()));

    // Fill in the necessary information. Incomplete information will result in NPE.
    session.setUsername(AuthorityChecker.SUPER_USER);
    session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    session.setZoneId(ZoneId.systemDefault());
  }

  @Override
  public void handshake() throws Exception {
    // Do nothing
  }

  @Override
  public void heartbeat() throws Exception {
    // Do nothing
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "WriteBackConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      doTransferWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else {
      doTransferWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
    }
  }

  private void doTransferWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        WriteBackConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          WriteBackConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    final InsertNode insertNode =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
    final String dataBaseName =
        pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
            ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
            : TREE_MODEL_DATABASE_NAME_IDENTIFIER;

    final InsertBaseStatement insertBaseStatement;
    if (Objects.isNull(insertNode)) {
      insertBaseStatement =
          PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
                  pipeInsertNodeTabletInsertionEvent.getByteBuffer(), dataBaseName)
              .constructStatement();
    } else {
      insertBaseStatement =
          PipeTransferTabletInsertNodeReqV2.toTabletInsertNodeReq(insertNode, dataBaseName)
              .constructStatement();
    }

    final TSStatus status =
        insertBaseStatement.isWriteToTable()
            ? executeStatementForTableModel(insertBaseStatement, dataBaseName)
            : executeStatementForTreeModel(insertBaseStatement);

    if (status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Write back PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, status));
    }
  }

  private void doTransferWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(WriteBackConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(WriteBackConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final String dataBaseName =
        pipeRawTabletInsertionEvent.isTableModelEvent()
            ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
            : TREE_MODEL_DATABASE_NAME_IDENTIFIER;

    final InsertTabletStatement insertTabletStatement =
        PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned(),
                dataBaseName)
            .constructStatement();

    final TSStatus status =
        insertTabletStatement.isWriteToTable()
            ? executeStatementForTableModel(insertTabletStatement, dataBaseName)
            : executeStatementForTreeModel(insertTabletStatement);

    if (status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Write back PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, status));
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    // Ignore the event except TabletInsertionEvent
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution);
    }
  }

  private TSStatus executeStatementForTableModel(Statement statement, String dataBaseName) {
    session.setDatabaseName(dataBaseName);
    session.setSqlDialect(IClientSession.SqlDialect.TABLE);
    SESSION_MANAGER.registerSession(session);
    try {
      autoCreateDatabaseIfNecessary(dataBaseName);
      return Coordinator.getInstance()
          .executeForTableModel(
              new PipeEnrichedStatement(statement),
              RELATIONAL_SQL_PARSER,
              session,
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfoOfPipeReceiver(session, dataBaseName),
              "",
              LocalExecutionPlanner.getInstance().metadata,
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
          .status;
    } catch (final Exception e) {
      ALREADY_CREATED_DATABASES.remove(dataBaseName);

      final Throwable rootCause = getRootCause(e);
      if (rootCause.getMessage() != null
          && rootCause
              .getMessage()
              .toLowerCase(Locale.ENGLISH)
              .contains(DATABASE_NOT_SET.toLowerCase(Locale.ENGLISH))) {
        autoCreateDatabaseIfNecessary(dataBaseName);

        // Retry after creating the database
        session.setDatabaseName(dataBaseName);
        return Coordinator.getInstance()
            .executeForTableModel(
                new PipeEnrichedStatement(statement),
                RELATIONAL_SQL_PARSER,
                session,
                SESSION_MANAGER.requestQueryId(),
                SESSION_MANAGER.getSessionInfo(session),
                "",
                LocalExecutionPlanner.getInstance().metadata,
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
            .status;
      }

      // If the exception is not caused by database not set, throw it directly
      throw e;
    } finally {
      SESSION_MANAGER.removeCurrSession();
    }
  }

  private void autoCreateDatabaseIfNecessary(final String database) {
    if (ALREADY_CREATED_DATABASES.contains(database)) {
      return;
    }

    final TDatabaseSchema schema = new TDatabaseSchema(new TDatabaseSchema(database));
    schema.setIsTableModel(true);

    final CreateDBTask task = new CreateDBTask(schema, true);
    try {
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      final int statusCode = result.getStatusCode().getStatusCode();
      if (statusCode != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && statusCode != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        throw new PipeException(
            String.format(
                "Auto create database failed: %s, status code: %s",
                database, result.getStatusCode()));
      }
    } catch (final ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new PipeException("Auto create database failed because: " + e.getMessage());
    }

    ALREADY_CREATED_DATABASES.add(database);
  }

  private TSStatus executeStatementForTreeModel(final Statement statement) {
    session.setDatabaseName(null);
    session.setSqlDialect(IClientSession.SqlDialect.TREE);
    SESSION_MANAGER.registerSession(session);
    try {
      return Coordinator.getInstance()
          .executeForTreeModel(
              new PipeEnrichedStatement(statement),
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(session),
              "",
              ClusterPartitionFetcher.getInstance(),
              ClusterSchemaFetcher.getInstance(),
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
              false)
          .status;
    } finally {
      SESSION_MANAGER.removeCurrSession();
    }
  }
}
