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
package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StartPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartPipeProcedureV2.class);

  private String pipeName;

  public StartPipeProcedureV2() {
    super();
  }

  public StartPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  @Override
  PipeTaskOperation getOperation() {
    return PipeTaskOperation.START_PIPE;
  }

  @Override
  boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("StartPipeProcedureV2: executeFromValidateTask({})", pipeName);

    return env.getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .getPipeTaskInfo()
        .checkBeforeStartPipe(pipeName);
  }

  @Override
  void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("StartPipeProcedureV2: executeFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeManagementException {
    LOGGER.info("StartPipeProcedureV2: executeFromWriteConfigNodeConsensus({})", pipeName);

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeManagementException, IOException {
    LOGGER.info("StartPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    if (RpcUtils.squashResponseStatusList(
                env.syncPipeMeta(
                    env.getConfigManager()
                        .getPipeManager()
                        .getPipeTaskCoordinator()
                        .getPipeTaskInfo()
                        .getPipeMeta(pipeName)))
            .getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to start pipe instance [%s] on data nodes", pipeName));
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromValidateTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})", pipeName);

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.STOPPED));
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeManagementException, IOException {
    LOGGER.info("StartPipeProcedureV2: rollbackFromOperateOnDataNodes({})", pipeName);

    PipeMeta pipeMeta =
        env.getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMeta(pipeName);
    pipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
    if (RpcUtils.squashResponseStatusList(env.syncPipeMeta(pipeMeta)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to rollback from start on data nodes for task [%s]", pipeName));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.START_PIPE_PROCEDURE_V2.getTypeCode());
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartPipeProcedureV2 that = (StartPipeProcedureV2) o;
    return pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }
}
