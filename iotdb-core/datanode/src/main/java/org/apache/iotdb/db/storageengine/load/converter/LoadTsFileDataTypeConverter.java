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

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Optional;

public class LoadTsFileDataTypeConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileDataTypeConverter.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  public static final LoadConvertedInsertTabletStatementTSStatusVisitor STATEMENT_STATUS_VISITOR =
      new LoadConvertedInsertTabletStatementTSStatusVisitor();
  public static final LoadTreeConvertedInsertTabletStatementExceptionVisitor
      TREE_STATEMENT_EXCEPTION_VISITOR =
          new LoadTreeConvertedInsertTabletStatementExceptionVisitor();
  public static final LoadTableConvertedInsertTabletStatementExceptionVisitor
      TABLE_STATEMENT_EXCEPTION_VISITOR =
          new LoadTableConvertedInsertTabletStatementExceptionVisitor();

  private final boolean isGeneratedByPipe;
  private final MPPQueryContext context;

  private final SqlParser relationalSqlParser = new SqlParser();
  private final LoadTableStatementDataTypeConvertExecutionVisitor
      tableStatementDataTypeConvertExecutionVisitor;
  private final LoadTreeStatementDataTypeConvertExecutionVisitor
      treeStatementDataTypeConvertExecutionVisitor;

  public LoadTsFileDataTypeConverter(
      final MPPQueryContext context, final boolean isGeneratedByPipe) {
    this.context = context;
    this.isGeneratedByPipe = isGeneratedByPipe;

    tableStatementDataTypeConvertExecutionVisitor =
        new LoadTableStatementDataTypeConvertExecutionVisitor(this::executeForTableModel);
    treeStatementDataTypeConvertExecutionVisitor =
        new LoadTreeStatementDataTypeConvertExecutionVisitor(this::executeForTreeModel);
  }

  public Optional<TSStatus> convertForTableModel(final LoadTsFile loadTsFileTableStatement) {
    try {
      return loadTsFileTableStatement.accept(
          tableStatementDataTypeConvertExecutionVisitor, loadTsFileTableStatement.getDatabase());
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to convert data types for table model statement {}.",
          loadTsFileTableStatement,
          e);
      return Optional.of(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    }
  }

  private TSStatus executeForTableModel(final Statement statement, final String databaseName) {
    final IClientSession session;
    final boolean needToCreateSession = SESSION_MANAGER.getCurrSession() == null;
    if (needToCreateSession) {
      session =
          new InternalClientSession(
              String.format(
                  "%s_%s",
                  LoadTsFileDataTypeConverter.class.getSimpleName(),
                  Thread.currentThread().getName()));
      session.setUsername(AuthorityChecker.SUPER_USER);
      session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
      session.setZoneId(ZoneId.systemDefault());
      session.setSqlDialect(IClientSession.SqlDialect.TABLE);

      SESSION_MANAGER.registerSession(session);
    } else {
      session = SESSION_MANAGER.getCurrSession();
    }
    try {
      return Coordinator.getInstance()
          .executeForTableModel(
              isGeneratedByPipe ? new PipeEnrichedStatement(statement) : statement,
              relationalSqlParser,
              session,
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfoOfPipeReceiver(session, databaseName),
              "",
              LocalExecutionPlanner.getInstance().metadata,
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
          .status;
    } finally {
      if (needToCreateSession) {
        SESSION_MANAGER.removeCurrSession();
      }
    }
  }

  public Optional<TSStatus> convertForTreeModel(final LoadTsFileStatement loadTsFileTreeStatement) {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    try {
      return loadTsFileTreeStatement.accept(treeStatementDataTypeConvertExecutionVisitor, null);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to convert data types for tree model statement {}.", loadTsFileTreeStatement, e);
      return Optional.of(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    } finally {
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TREE);
    }
  }

  private TSStatus executeForTreeModel(final Statement statement) {
    final IClientSession session;
    final boolean needToCreateSession = SESSION_MANAGER.getCurrSession() == null;
    if (needToCreateSession) {
      session =
          new InternalClientSession(
              String.format(
                  "%s_%s",
                  LoadTsFileDataTypeConverter.class.getSimpleName(),
                  Thread.currentThread().getName()));
      session.setUsername(AuthorityChecker.SUPER_USER);
      session.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
      session.setZoneId(ZoneId.systemDefault());
      session.setSqlDialect(IClientSession.SqlDialect.TREE);

      SESSION_MANAGER.registerSession(session);
    } else {
      session = SESSION_MANAGER.getCurrSession();
    }

    try {
      return Coordinator.getInstance()
          .executeForTreeModel(
              isGeneratedByPipe ? new PipeEnrichedStatement(statement) : statement,
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfoOfTreeModel(session),
              "",
              ClusterPartitionFetcher.getInstance(),
              ClusterSchemaFetcher.getInstance(),
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
              false)
          .status;
    } finally {
      if (needToCreateSession) {
        SESSION_MANAGER.removeCurrSession();
      }
    }
  }

  public boolean isSuccessful(final TSStatus status) {
    return status != null
        && (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
            || status.getCode() == TSStatusCode.LOAD_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode());
  }
}
