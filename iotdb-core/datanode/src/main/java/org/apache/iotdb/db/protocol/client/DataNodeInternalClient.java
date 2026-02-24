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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.IntoProcessException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class DataNodeInternalClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeInternalClient.class);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final IClientSession session;

  // Relational Insert
  private final SqlParser relationSqlParser;
  private final Metadata metadata;

  public DataNodeInternalClient(SessionInfo sessionInfo) {
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
    metadata = LocalExecutionPlanner.getInstance().metadata;
    relationSqlParser = new SqlParser();

    try {
      session = new InternalClientSession("SELECT_INTO");
      session.setSqlDialect(sessionInfo.getSqlDialect());

      SESSION_MANAGER.supplySession(
          session,
          sessionInfo.getUserId(),
          sessionInfo.getUserName(),
          sessionInfo.getZoneId(),
          ClientVersion.V_1_0);

      LOGGER.info("User: {}, opens internal Session-{}.", sessionInfo.getUserName(), session);
    } catch (Exception e) {
      LOGGER.warn("User {} opens internal Session failed.", sessionInfo.getUserName(), e);
      throw new IntoProcessException(
          String.format("User %s opens internal Session failed.", sessionInfo.getUserName()));
    }
  }

  public TSStatus insertTablets(InsertMultiTabletsStatement statement) {
    try {
      // permission check
      TSStatus status =
          AuthorityChecker.checkAuthority(
              statement,
              new TreeAccessCheckContext(
                  session.getUserId(), session.getUsername(), session.getClientAddress()));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              partitionFetcher,
              schemaFetcher);
      return result.status;
    } catch (final Exception e) {
      return onQueryException(
          e, OperationType.INSERT_TABLETS.getName(), TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  public TSStatus insertRelationalTablet(InsertTabletStatement statement) {
    long startTime = System.nanoTime();
    try {
      // call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              statement,
              relationSqlParser,
              session,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              metadata,
              config.getConnectionTimeoutInMS());
      return result.status;
    } catch (Exception e) {
      return onQueryException(
          e, OperationType.SELECT_INTO.getName(), TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      CommonUtils.addStatementExecutionLatency(
          OperationType.SELECT_INTO,
          StatementType.SELECT_INTO.name(),
          System.nanoTime() - startTime);
    }
  }

  public void close() {
    SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution);
  }
}
