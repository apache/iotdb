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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.IntoProcessException;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.control.clientsession.InternalClientSession;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;

public class DataNodeInternalClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeInternalClient.class);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;

  private final IClientSession session;

  public DataNodeInternalClient(SessionInfo sessionInfo) {
    PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
    SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();

    try {
      session = new InternalClientSession("SELECT_INTO");

      SESSION_MANAGER.supplySession(
          session, sessionInfo.getUserName(), sessionInfo.getZoneId(), ClientVersion.V_1_0);

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
      TSStatus status = AuthorityChecker.checkAuthority(statement, session);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);
      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLETS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  public void close() {
    SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution);
  }
}
