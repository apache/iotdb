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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StartPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartPipeProcedureV2.class);

  private String pipeName;
  private boolean isTableModel;
  private boolean isTableModelSet;

  public StartPipeProcedureV2() {
    super();
  }

  public StartPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  public StartPipeProcedureV2(String pipeName, boolean isTableModel) throws PipeException {
    super();
    this.pipeName = pipeName;
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.START_PIPE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK, pipeName);

    if (isTableModelSet) {
      pipeTaskInfo.get().checkBeforeStartPipe(pipeName, isTableModel);
      return !pipeTaskInfo.get().isPipeRunning(pipeName, isTableModel)
          || pipeTaskInfo.get().isStoppedByRuntimeException(pipeName, isTableModel);
    }

    pipeTaskInfo.get().checkBeforeStartPipe(pipeName);
    return !pipeTaskInfo.get().isPipeRunning(pipeName)
        || pipeTaskInfo.get().isStoppedByRuntimeException(pipeName);
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK, pipeName);
    // Do nothing
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        ProcedureMessages.STARTPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS, pipeName);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isTableModelSet
                      ? new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING, isTableModel)
                      : new SetPipeStatusPlanV2(pipeName, PipeStatus.RUNNING));
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES, pipeName);

    final long exceptionsClearTime = System.currentTimeMillis();
    final boolean isStoppedByRuntimeException =
        isTableModelSet
            ? pipeTaskInfo.get().isStoppedByRuntimeException(pipeName, isTableModel)
            : pipeTaskInfo.get().isStoppedByRuntimeException(pipeName);
    final PipeStaticMeta pipeStaticMeta =
        (isTableModelSet
                ? pipeTaskInfo.get().getPipeMetaByPipeName(pipeName, isTableModel)
                : pipeTaskInfo.get().getPipeMetaByPipeName(pipeName))
            .getStaticMeta();
    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(
            pipeName, pushSinglePipeMetaToDataNodes(pipeStaticMeta, env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER,
          pipeName,
          exceptionMessage);
      return;
    }

    // Clear exceptions and set isStoppedByRuntimeException to false if the pipe is
    // started successfully on all data nodes
    if (isTableModelSet) {
      pipeTaskInfo
          .get()
          .clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(
              pipeName, isTableModel, exceptionsClearTime);
    } else {
      pipeTaskInfo
          .get()
          .clearExceptionsAndSetIsStoppedByRuntimeExceptionToFalse(pipeName, exceptionsClearTime);
    }

    if (isStoppedByRuntimeException) {
      writePipeMetaChangesToConfigNodeConsensus(env);
    }
  }

  private void writePipeMetaChangesToConfigNodeConsensus(final ConfigNodeProcedureEnv env) {
    final List<PipeMeta> pipeMetaList = new ArrayList<>();
    for (final PipeMeta pipeMeta : pipeTaskInfo.get().getPipeMetaList()) {
      pipeMetaList.add(pipeMeta);
    }

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new PipeHandleMetaChangePlan(pipeMetaList));
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK, pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK, pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        ProcedureMessages.STARTPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS, pipeName);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isTableModelSet
                      ? new SetPipeStatusPlanV2(pipeName, PipeStatus.STOPPED, isTableModel)
                      : new SetPipeStatusPlanV2(pipeName, PipeStatus.STOPPED));
    } catch (ConsensusException e) {
      LOGGER.warn(ConfigNodeMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info(ProcedureMessages.STARTPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES, pipeName);

    // Push all pipe metas to datanode, may be time-consuming
    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_ROLLBACK_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED,
          pipeName,
          exceptionMessage);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.START_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
    if (isTableModelSet) {
      ReadWriteIOUtils.write(isTableModel, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
    isTableModelSet = byteBuffer.hasRemaining();
    if (isTableModelSet) {
      isTableModel = ReadWriteIOUtils.readBool(byteBuffer);
    }
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
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && getCycles() == that.getCycles()
        && isTableModel == that.isTableModel
        && isTableModelSet == that.isTableModelSet
        && pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), pipeName, isTableModel, isTableModelSet);
  }
}
