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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

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
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.START_PIPE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("StartPipeProcedureV2: executeFromValidateTask({})", pipeName);

    pipeTaskInfo.get().checkBeforeStartPipe(pipeName);

    return !pipeTaskInfo.get().isPipeRunning(pipeName)
        || pipeTaskInfo.get().isStoppedByRuntimeException(pipeName);
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("StartPipeProcedureV2: executeFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("StartPipeProcedureV2: executeFromWriteConfigNodeConsensus({})", pipeName);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("StartPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushSinglePipeMetaToDataNodes(pipeName, env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to start pipe {}, details: {}, metadata will be synchronized later.",
          pipeName,
          exceptionMessage);
      return;
    }

    // Clear exceptions and set isStoppedByRuntimeException to false if the pipe is
    // started successfully on all data nodes
    pipeTaskInfo.get().clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(pipeName);
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromValidateTask({})", pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("StartPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})", pipeName);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.STOPPED));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("StartPipeProcedureV2: rollbackFromOperateOnDataNodes({})", pipeName);

    // Push all pipe metas to datanode, may be time-consuming
    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to rollback start pipe {}, details: {}, metadata will be synchronized later.",
          pipeName,
          exceptionMessage);
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
    return getProcId() == that.getProcId()
        && getCurrentState().equals(that.getCurrentState())
        && getCycles() == that.getCycles()
        && pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getCurrentState(), getCycles(), pipeName);
  }
}
