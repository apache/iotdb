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
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.state.pipe.task.OperatePipeTaskState;
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
  private PipeMeta pipeMetaToDrop;

  public DropPipeProcedureV2() {
    super();
  }

  public DropPipeProcedureV2(String pipeName) throws PipeException {
    super();
    this.pipeName = pipeName;
  }

  /** This is only used when the pipe task info lock is held by another procedure. */
  public DropPipeProcedureV2(String pipeName, AtomicReference<PipeTaskInfo> pipeTaskInfo)
      throws PipeException {
    super();
    this.pipeName = pipeName;
    this.pipeTaskInfo = pipeTaskInfo;
  }

  public String getPipeName() {
    return pipeName;
  }

  PipeMeta getPipeMetaToDrop() {
    return pipeMetaToDrop;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.DROP_PIPE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeProcedureV2: executeFromValidateTask({})", pipeName);

    pipeTaskInfo.get().checkBeforeDropPipe(pipeName);

    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeProcedureV2: executeFromCalculateInfoForTask({})", pipeName);
    pipeMetaToDrop = pipeTaskInfo.get().getPipeMetaByPipeName(pipeName);
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeProcedureV2: executeFromWriteConfigNodeConsensus({})", pipeName);

    if (!restorePipeMetaToDropIfNecessary()) {
      return;
    }

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new SetPipeStatusPlanV2(pipeName, PipeStatus.PRE_DELETE));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  boolean restorePipeMetaToDropIfNecessary() {
    if (pipeMetaToDrop == null) {
      pipeMetaToDrop = pipeTaskInfo.get().getPipeMetaByPipeName(pipeName);
    }
    return pipeMetaToDrop != null;
  }

  private void dropPipeOnConfigNode(final ConfigNodeProcedureEnv env) throws PipeException {
    TSStatus response;
    try {
      response = env.getConfigManager().getConsensusManager().write(new DropPipePlanV2(pipeName));
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
  public void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("DropPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    String exceptionMessage;
    try {
      if (pipeMetaToDrop == null) {
        exceptionMessage =
            parsePushPipeMetaExceptionForPipe(pipeName, dropSinglePipeOnDataNodes(pipeName, env));
      } else {
        final PipeMeta droppedPipeMeta = pipeMetaToDrop.deepCopy4TaskAgent();
        droppedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);
        exceptionMessage =
            parsePushPipeMetaExceptionForPipe(
                pipeName,
                env.pushSinglePipeMetaToDataNodes(
                    droppedPipeMeta.serialize(), this::setPendingDataNodeIds));
      }
    } catch (final IOException e) {
      exceptionMessage = e.getMessage();
    }
    if (exceptionMessage != null && !exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to drop pipe {}, details: {}, metadata will be synchronized later.",
          pipeName,
          exceptionMessage);
    }
    updateExecutionStage(OperatePipeTaskState.WRITE_CONFIG_NODE_CONSENSUS, false);
    dropPipeOnConfigNode(env);
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromValidateTask({})", pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromCalculateInfoForTask({})", pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})", pipeName);
    // Do nothing
  }

  @Override
  public void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("DropPipeProcedureV2: rollbackFromOperateOnDataNodes({})", pipeName);
    // Do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
    ReadWriteIOUtils.write(SERIALIZATION_VERSION_MAGIC, stream);
    ReadWriteIOUtils.write(pipeMetaToDrop != null, stream);
    if (pipeMetaToDrop != null) {
      pipeMetaToDrop.serialize(stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
    if (byteBuffer.remaining() < Integer.BYTES) {
      return;
    }
    final int position = byteBuffer.position();
    if (ReadWriteIOUtils.readInt(byteBuffer) != SERIALIZATION_VERSION_MAGIC) {
      byteBuffer.position(position);
      return;
    }
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
        && pipeName.equals(that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getCurrentState(), getCycles(), pipeName);
  }
}
