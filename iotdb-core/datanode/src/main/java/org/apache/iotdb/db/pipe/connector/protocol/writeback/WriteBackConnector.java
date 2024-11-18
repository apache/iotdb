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
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

public class WriteBackConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackConnector.class);

  private static final String EMPTY_DATABASE = null;
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final SqlParser relationalSqlParser = new SqlParser();

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    // Do nothing
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

  @Override
  public void transfer(final Event event) throws Exception {
    if (!(event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent)) {
      LOGGER.warn("WriteBackConnector does not support transferring generic event: {}.", event);
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
    final TSStatus status;
    final boolean isTableModel = pipeInsertNodeTabletInsertionEvent.isTableModelEvent();
    final String dataBaseName =
        isTableModel
            ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
            : EMPTY_DATABASE;
    final InsertNode insertNode =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
    if (Objects.isNull(insertNode)) {
      status =
          PipeDataNodeAgent.receiver()
              .thrift()
              .receive(
                  PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
                      pipeInsertNodeTabletInsertionEvent.getByteBuffer(), dataBaseName))
              .getStatus();
    } else {
      final InsertBaseStatement statement =
          PipeTransferTabletInsertNodeReqV2.toTabletInsertNodeReq(
                  insertNode, pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName())
              .constructStatement();
      status = statement.isEmpty() ? RpcUtils.SUCCESS_STATUS : executeTreeModelStatement(statement);
    }

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
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
    final boolean isTableModel = pipeRawTabletInsertionEvent.isTableModelEvent();
    final String dataBaseName =
        isTableModel ? pipeRawTabletInsertionEvent.getTableModelDatabaseName() : EMPTY_DATABASE;
    final InsertBaseStatement statement =
        PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned(),
                dataBaseName)
            .constructStatement();
    final TSStatus status;
    if (isTableModel) {
      status =
          statement.isEmpty() ? RpcUtils.SUCCESS_STATUS : executeTableModelStatement(statement);
    } else {
      status = statement.isEmpty() ? RpcUtils.SUCCESS_STATUS : executeTreeModelStatement(statement);
    }

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, status));
    }
  }

  private TSStatus executeTreeModelStatement(final InsertBaseStatement statement) {
    return Coordinator.getInstance()
        .executeForTreeModel(
            new PipeEnrichedStatement(statement),
            SESSION_MANAGER.requestQueryId(),
            new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
            "",
            ClusterPartitionFetcher.getInstance(),
            ClusterSchemaFetcher.getInstance(),
            IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
        .status;
  }

  private TSStatus executeTableModelStatement(final InsertBaseStatement statement) {
    return Coordinator.getInstance()
        .executeForTableModel(
            new PipeEnrichedStatement(statement),
            relationalSqlParser,
            SESSION_MANAGER.getCurrSession(),
            SESSION_MANAGER.requestQueryId(),
            SESSION_MANAGER.getSessionInfoOfPipeReceiver(
                SESSION_MANAGER.getCurrSession(),
                statement.getDatabaseName().isPresent()
                    ? statement.getDatabaseName().get()
                    : EMPTY_DATABASE),
            "",
            LocalExecutionPlanner.getInstance().metadata,
            IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold())
        .status;
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
