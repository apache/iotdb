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
import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.utils.sync.SyncPipeUtil;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreatePipeProcedure extends AbstractOperatePipeProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeProcedure.class);

  private PipeInfo pipeInfo;

  public CreatePipeProcedure() {
    super();
  }

  public CreatePipeProcedure(TCreatePipeReq req) throws PipeException {
    super();
    this.pipeInfo = SyncPipeUtil.parseTCreatePipeReqAsPipeInfo(req, System.currentTimeMillis());
  }

  @Override
  boolean executeCheckCanSkip(ConfigNodeProcedureEnv env) throws PipeException, PipeSinkException {
    LOGGER.info("Start to create PIPE [{}]", pipeInfo.getPipeName());
    env.getConfigManager().getSyncManager().checkAddPipe(pipeInfo);
    return false;
  }

  @Override
  void executePreOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to pre-create PIPE [{}] on Config Nodes", pipeInfo.getPipeName());
    TSStatus status = env.getConfigManager().getSyncManager().preCreatePipe(pipeInfo);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  void executeOperatePipeOnDataNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to broadcast create PIPE [{}] on Data Nodes", pipeInfo.getPipeName());
    TSStatus status =
        RpcUtils.squashResponseStatusList(
            env.getConfigManager().getSyncManager().preCreatePipeOnDataNodes(pipeInfo));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Fail to create PIPE [%s] on Data Nodes because %s",
              pipeInfo.getPipeName(), status.getMessage()));
    }
  }

  @Override
  void executeOperatePipeOnConfigNode(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("Start to create PIPE [{}] on Config Nodes", pipeInfo.getPipeName());
    TSStatus status =
        env.getConfigManager()
            .getSyncManager()
            .setPipeStatus(pipeInfo.getPipeName(), PipeStatus.STOP);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(status.getMessage());
    }
  }

  @Override
  SyncOperation getOperation() {
    return SyncOperation.CREATE_PIPE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeState state) {
    switch (state) {
      case PRE_OPERATE_PIPE_CONFIGNODE:
      case OPERATE_PIPE_DATANODE:
        return true;
    }
    return false;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws IOException, InterruptedException, ProcedureException {
    LOGGER.error("Roll back CreatePipeProcedure at STATE [{}]", state);
    env.getConfigManager().getSyncManager().unlockSyncMetadata();
    // TODO(sync): roll back logic;
    switch (state) {
      case PRE_OPERATE_PIPE_CONFIGNODE:
        TSStatus status = env.getConfigManager().getSyncManager().dropPipe(pipeInfo.getPipeName());
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new ProcedureException(
              String.format(
                  "Failed to create pipe and failed to roll back because %s. Please execute [DROP PIPE %s] manually.",
                  status.getMessage(), pipeInfo.getPipeName()));
        }
        break;
      case OPERATE_PIPE_DATANODE:
        status =
            RpcUtils.squashResponseStatusList(
                env.getConfigManager()
                    .getSyncManager()
                    .operatePipeOnDataNodes(pipeInfo.getPipeName(), SyncOperation.DROP_PIPE));
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          status = env.getConfigManager().getSyncManager().dropPipe(pipeInfo.getPipeName());
        }
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new ProcedureException(
              String.format(
                  "Failed to create pipe and failed to roll back because %s. Please execute [DROP PIPE %s] manually.",
                  status.getMessage(), pipeInfo.getPipeName()));
        }
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
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
