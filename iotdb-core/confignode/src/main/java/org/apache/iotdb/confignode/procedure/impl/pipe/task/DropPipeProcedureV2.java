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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
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
import java.util.concurrent.atomic.AtomicReference;

public class DropPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropPipeProcedureV2.class);
  private static final int SERIALIZATION_VERSION_MAGIC = 0x44505632;

  private String pipeName;
  private boolean isTableModel;
  private boolean isTableModelSet;
  private PipeMeta pipeMetaToDrop;

  public DropPipeProcedureV2() {
    super();
  }

  public DropPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  public DropPipeProcedureV2(String pipeName, boolean isTableModel) throws PipeException {
    super();
    this.pipeName = pipeName;
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
  }

  /** This is only used when the pipe task info lock is held by another procedure. */
  public DropPipeProcedureV2(String pipeName, AtomicReference<PipeTaskInfo> pipeTaskInfo)
      throws PipeException {
    super();
    this.pipeName = pipeName;
    this.pipeTaskInfo = pipeTaskInfo;
  }

  /** This is only used when the pipe task info lock is held by another procedure. */
  public DropPipeProcedureV2(
      String pipeName, boolean isTableModel, AtomicReference<PipeTaskInfo> pipeTaskInfo)
      throws PipeException {
    super();
    this.pipeName = pipeName;
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
    this.pipeTaskInfo = pipeTaskInfo;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean isTableModel() {
    return pipeMetaToDrop == null
        ? isTableModel
        : pipeMetaToDrop.getStaticMeta().visibleUnderTableModel();
  }

  public boolean isTableModelSet() {
    return isTableModelSet;
  }

  public PipeMeta getPipeMetaToDrop() {
    return pipeMetaToDrop;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_PIPE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK, pipeName);

    pipeTaskInfo.get().checkBeforeDropPipe(pipeName);

    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK, pipeName);
    pipeMetaToDrop =
        isTableModelSet
            ? pipeTaskInfo.get().getPipeMetaByPipeName(pipeName, isTableModel)
            : pipeTaskInfo.get().getPipeMetaByPipeName(pipeName);
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        ProcedureMessages.DROPPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS, pipeName);

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isTableModelSet
                      ? new DropPipePlanV2(pipeName, isTableModel)
                      : new DropPipePlanV2(pipeName));
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
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES, pipeName);

    String exceptionMessage;
    try {
      if (pipeMetaToDrop == null) {
        exceptionMessage =
            parsePushPipeMetaExceptionForPipe(pipeName, pushPipeMetaToDataNodes(env));
      } else {
        final PipeMeta droppedPipeMeta =
            copyAndFilterOutNonWorkingDataRegionPipeTasks(pipeMetaToDrop);
        droppedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);
        exceptionMessage =
            parsePushPipeMetaExceptionForPipe(
                pipeName, env.pushSinglePipeMetaToDataNodes(droppedPipeMeta.serialize()));
      }
    } catch (final IOException e) {
      exceptionMessage = e.getMessage();
    }
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_DROP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER,
          pipeName,
          exceptionMessage);
    }
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK, pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK, pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        ProcedureMessages.DROPPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS, pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info(ProcedureMessages.DROPPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES, pipeName);
    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write(SERIALIZATION_VERSION_MAGIC, stream);
    ReadWriteIOUtils.write(isTableModelSet, stream);
    if (isTableModelSet) {
      ReadWriteIOUtils.write(isTableModel, stream);
      if (pipeMetaToDrop == null) {
        ReadWriteIOUtils.write(false, stream);
      } else {
        ReadWriteIOUtils.write(true, stream);
        pipeMetaToDrop.serialize(stream);
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
    if (!byteBuffer.hasRemaining()) {
      return;
    }
    if (byteBuffer.remaining() >= Integer.BYTES) {
      final int position = byteBuffer.position();
      if (ReadWriteIOUtils.readInt(byteBuffer) == SERIALIZATION_VERSION_MAGIC) {
        if (!byteBuffer.hasRemaining()) {
          return;
        }
        isTableModelSet = ReadWriteIOUtils.readBool(byteBuffer);
        if (isTableModelSet) {
          isTableModel = ReadWriteIOUtils.readBool(byteBuffer);
          if (ReadWriteIOUtils.readBool(byteBuffer)) {
            pipeMetaToDrop = PipeMeta.deserialize4Coordinator(byteBuffer);
          }
        }
        return;
      }
      byteBuffer.position(position);
    }
    isTableModel = ReadWriteIOUtils.readBool(byteBuffer);
    isTableModelSet = true;
    if (byteBuffer.hasRemaining() && ReadWriteIOUtils.readBool(byteBuffer)) {
      pipeMetaToDrop = PipeMeta.deserialize4Coordinator(byteBuffer);
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
    DropPipeProcedureV2 that = (DropPipeProcedureV2) o;
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
