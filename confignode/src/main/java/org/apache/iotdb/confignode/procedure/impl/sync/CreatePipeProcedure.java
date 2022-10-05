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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.sync.CreatePipeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TPipeInfo;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreatePipeProcedure extends AbstractNodeProcedure<CreatePipeState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeProcedure.class);
  private static final int retryThreshold = 5;

  private PipeInfo pipeInfo;

  public CreatePipeProcedure() {
    super();
  }

  public CreatePipeProcedure(TPipeInfo pipeInfo) throws PipeException {
    super();
    this.pipeInfo = SyncPipeUtil.parseTPipeInfoAsPipeInfo(pipeInfo, System.currentTimeMillis());
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreatePipeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (pipeInfo == null) {
      return Flow.NO_MORE_STATE;
    }
    try {
      switch (state) {
        case CREATE_CHECK:
          LOGGER.info("Start to create PIPE [{}]", pipeInfo.getPipeName());
          env.getConfigManager().getSyncManager().checkAddPipe(pipeInfo);
          setNextState(CreatePipeState.PRE_CREATE_PIPE_CONFIGNODE);
          break;
        case PRE_CREATE_PIPE_CONFIGNODE:
          LOGGER.info("Start to pre-create PIPE [{}] on Config Nodes", pipeInfo.getPipeName());
          TSStatus status = env.getConfigManager().getSyncManager().preCreatePipe(pipeInfo);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new PipeException(status.getMessage());
          }
          setNextState(CreatePipeState.CREATE_PIPE_DATANODE);
          break;
        case CREATE_PIPE_DATANODE:
          LOGGER.info("Start to broadcast create PIPE [{}] on Data Nodes", pipeInfo.getPipeName());
          if (RpcUtils.squashResponseStatusList(env.preCreatePipeOnDataNodes(pipeInfo)).getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            setNextState(CreatePipeState.CREATE_PIPE_CONFIGNODE);
          } else {
            throw new PipeException(
                String.format("Fail to create PIPE [%s] on Data Nodes", pipeInfo.getPipeName()));
          }
          break;
        case CREATE_PIPE_CONFIGNODE:
          LOGGER.info("Start to create PIPE [{}] on Config Nodes", pipeInfo.getPipeName());
          status =
              env.getConfigManager()
                  .getSyncManager()
                  .operatePipe(pipeInfo.getPipeName(), SyncOperation.CREATE_PIPE);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new PipeException(status.getMessage());
          }
          return Flow.NO_MORE_STATE;
      }
    } catch (PipeException e) {
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in CreatePipeProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to create PIPE [{}], state [{}]",
            pipeInfo.getPipeName(),
            state,
            e);
        if (getCycles() > retryThreshold) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to create PIPE %s because %s",
                      pipeInfo.getPipeName(), e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean isRollbackSupported(CreatePipeState state) {
    switch (state) {
      case CREATE_CHECK:
      case CREATE_PIPE_DATANODE:
        return true;
    }
    return false;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv configNodeProcedureEnv, CreatePipeState state)
      throws IOException, InterruptedException, ProcedureException {
    LOGGER.error("Roll back CreatePipeProcedure at STATE [{}]", state);
    // TODO(sync): roll back logic;
  }

  @Override
  protected CreatePipeState getState(int stateId) {
    return CreatePipeState.values()[stateId];
  }

  @Override
  protected int getStateId(CreatePipeState createPipeState) {
    return createPipeState.ordinal();
  }

  @Override
  protected CreatePipeState getInitialState() {
    return CreatePipeState.CREATE_CHECK;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.CREATE_PIPE_PROCEDURE.ordinal());
    super.serialize(stream);
    pipeInfo.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeInfo = PipeInfo.deserializePipeInfo(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreatePipeProcedure that = (CreatePipeProcedure) o;
    return Objects.equals(pipeInfo, that.pipeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeInfo);
  }
}
