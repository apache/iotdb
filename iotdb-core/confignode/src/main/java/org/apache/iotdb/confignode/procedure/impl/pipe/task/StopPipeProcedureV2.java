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

import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StopPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(StopPipeProcedureV2.class);

  private String pipeName;

  public StopPipeProcedureV2() {
    super();
  }

  public StopPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.STOP_PIPE;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("StopPipeProcedureV2: executeFromValidateTask({})", pipeName);

    env.getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .getPipeTaskInfo()
        .checkBeforeStopPipe(pipeName);
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("StopPipeProcedureV2: executeFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException {
    LOGGER.info("StopPipeProcedureV2: executeFromWriteConfigNodeConsensus({})", pipeName);

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.STOPPED));
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException {
    LOGGER.info("StopPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      throw new PipeException(
          String.format("Failed to stop pipe %s, details: %s", pipeName, exceptionMessage));
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StopPipeProcedureV2: rollbackFromValidateTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StopPipeProcedureV2: rollbackFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("StopPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})", pipeName);

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException {
    LOGGER.info("StopPipeProcedureV2: rollbackFromOperateOnDataNodes({})", pipeName);

    String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      throw new PipeException(
          String.format(
              "Failed to rollback stop pipe %s, details: %s", pipeName, exceptionMessage));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.STOP_PIPE_PROCEDURE_V2.getTypeCode());
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
    StopPipeProcedureV2 that = (StopPipeProcedureV2) o;
    return pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return pipeName.hashCode();
  }
}
