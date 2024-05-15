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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsensusPipeDataNodeDispatcher implements ConsensusPipeDispatcher {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusPipeDataNodeDispatcher.class);

  private TSStatus executePipeStatement(Statement statement) {
    final long queryId = SessionManager.getInstance().requestQueryId();
    final ExecutionResult result =
        Coordinator.getInstance()
            .executeForTreeModel(
                statement,
                queryId,
                null,
                "",
                null,
                null,
                IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
    return result.status;
  }

  @Override
  public void createPipe(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    final CreatePipeStatement statement = new CreatePipeStatement(StatementType.CREATE_PIPE);
    statement.setPipeName(pipeName);
    statement.setExtractorAttributes(extractorAttributes);
    statement.setProcessorAttributes(processorAttributes);
    statement.setConnectorAttributes(connectorAttributes);

    final TSStatus status = executePipeStatement(statement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to create pipe, status: {}", status);
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  public void startPipe(String pipeName) throws Exception {
    final StartPipeStatement statement = new StartPipeStatement(StatementType.START_PIPE);
    statement.setPipeName(pipeName);

    final TSStatus status = executePipeStatement(statement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to start pipe, status: {}", status);
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  public void stopPipe(String pipeName) throws Exception {
    final StopPipeStatement statement = new StopPipeStatement(StatementType.STOP_PIPE);
    statement.setPipeName(pipeName);

    final TSStatus status = executePipeStatement(statement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to stop pipe, status: {}", status);
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  public void dropPipe(String pipeName) throws Exception {
    final DropPipeStatement statement = new DropPipeStatement(StatementType.DROP_PIPE);
    statement.setPipeName(pipeName);

    final TSStatus status = executePipeStatement(statement);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to drop pipe, status: {}", status);
      throw new PipeException(status.getMessage());
    }
  }
}
