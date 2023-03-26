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

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StopPipeProcedure extends AbstractOperatePipeProcedure {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartPipeProcedure.class);

  private String pipeName;

  public StopPipeProcedure() {
    super();
  }

  public StopPipeProcedure(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  @Override
  boolean validateTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to validate PIPE [{}]", pipeName);
    return env.getConfigManager()
        .getPipeManager()
        .getPipeInfo()
        .checkOperatePipeTask(pipeName, SyncOperation.STOP_PIPE);
  }

  @Override
  void calculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    // Do nothing
  }

  @Override
  void operateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to broadcast stop PIPE [{}] on Data Nodes", pipeName);

    TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq()
            .setPipeName(pipeName)
            .setOperation((byte) SyncOperation.STOP_PIPE.ordinal());
    if (RpcUtils.squashResponseStatusList(env.operatePipeOnDataNodes(request)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to stop pipe instance [%s] on data nodes", pipeName));
    }
  }

  @Override
  void writeConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to stop PIPE [{}] on Config Nodes", pipeName);

    final ConfigManager configNodeManager = env.getConfigManager();

    final SetPipeStatusPlanV2 setPipeStatusPlanV2 =
        new SetPipeStatusPlanV2(pipeName, PipeStatus.STOP);

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(setPipeStatusPlanV2);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  SyncOperation getOperation() {
    return SyncOperation.STOP_PIPE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeState state) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws IOException, InterruptedException, ProcedureException {
    LOGGER.info("Roll back CreatePipeProcedure at STATE [{}]", state);
    switch (state) {
      case VALIDATE_TASK:
        rollbackFromValidateTask(env);
        break;
      case CALCULATE_INFO_FOR_TASK:
        rollbackFromCalculateInfoForTask(env);
        break;
      case OPERATE_ON_DATA_NODES:
        rollbackFromOperateOnDataNodes(env);
        break;
      case WRITE_CONFIG_NODE_CONSENSUS:
        rollbackFromWriteConfigNodeConsensus(env);
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  private void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to rollback from validate task [{}]", pipeName);
    env.getConfigManager().getPipeManager().unlockPipeTaskInfo();
  }

  private void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    // Do nothing
  }

  private void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeManagementException {
    LOGGER.info("Start to rollback from operate on data nodes for task [{}]", pipeName);

    // Stop pipe
    TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq()
            .setPipeName(pipeName)
            .setOperation((byte) SyncOperation.START_PIPE.ordinal());
    if (RpcUtils.squashResponseStatusList(env.operatePipeOnDataNodes(request)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to create pipe instance [%s] on data nodes", pipeName));
    }
  }

  private void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to rollback from write config node consensus for task [{}]", pipeName);

    // Stop pipe
    final ConfigManager configNodeManager = env.getConfigManager();

    final SetPipeStatusPlanV2 setPipeStatusPlanV2 =
        new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING);

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(setPipeStatusPlanV2);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.STOP_PIPE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StopPipeProcedure that = (StopPipeProcedure) o;
    return pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }
}
