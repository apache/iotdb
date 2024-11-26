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
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class WriteBackConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteBackConnector.class);

  // Simulate the behavior of the client-to-server communication
  // for correctly handling data insertion in IoTDBReceiverAgent#receive method
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private IClientSession session;

  private static final String TREE_MODEL_DATABASE_NAME_IDENTIFIER = null;

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
    SESSION_MANAGER.registerSession(session);
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

    final TSStatus status =
        PipeDataNodeAgent.receiver()
            .thrift()
            .receive(
                Objects.isNull(insertNode)
                    ? PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
                        pipeInsertNodeTabletInsertionEvent.getByteBuffer(), dataBaseName)
                    : PipeTransferTabletInsertNodeReqV2.toTabletInsertNodeReq(
                        insertNode, dataBaseName))
            .getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
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
    final TSStatus status =
        PipeDataNodeAgent.receiver()
            .thrift()
            .receive(
                PipeTransferTabletRawReqV2.toTPipeTransferRawReq(
                    pipeRawTabletInsertionEvent.convertToTablet(),
                    pipeRawTabletInsertionEvent.isAligned(),
                    pipeRawTabletInsertionEvent.isTableModelEvent()
                        ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                        : TREE_MODEL_DATABASE_NAME_IDENTIFIER))
            .getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
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
    SESSION_MANAGER.removeCurrSession();
  }
}
