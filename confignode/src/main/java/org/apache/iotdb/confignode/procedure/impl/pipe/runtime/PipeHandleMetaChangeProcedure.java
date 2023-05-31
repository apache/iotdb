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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.response.pipe.task.PipeTableResp;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PipeHandleMetaChangeProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHandleMetaChangeProcedure.class);

  private int dataNodeId;
  private final List<ByteBuffer> pipeMetaByteBufferListFromDataNode;

  private boolean needWriteConsensusOnConfigNodes = false;
  private boolean needPushPipeMetaToDataNodes = false;

  public PipeHandleMetaChangeProcedure() {
    super();
    pipeMetaByteBufferListFromDataNode = new ArrayList<>();
  }

  public PipeHandleMetaChangeProcedure(
      int dataNodeId, @NotNull List<ByteBuffer> pipeMetaByteBufferListFromDataNode) {
    super();
    this.dataNodeId = dataNodeId;
    this.pipeMetaByteBufferListFromDataNode = pipeMetaByteBufferListFromDataNode;
    needWriteConsensusOnConfigNodes = false;
    needPushPipeMetaToDataNodes = false;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.HANDLE_PIPE_META_CHANGE;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromValidateTask");

    // do nothing
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromCalculateInfoForTask");

    final Map<PipeStaticMeta, PipeMeta> pipeMetaMapFromDataNode = new HashMap<>();
    for (ByteBuffer byteBuffer : pipeMetaByteBufferListFromDataNode) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(byteBuffer);
      pipeMetaMapFromDataNode.put(pipeMeta.getStaticMeta(), pipeMeta);
    }

    for (final PipeMeta pipeMetaOnConfigNode :
        env.getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMetaList()) {
      final PipeMeta pipeMetaFromDataNode =
          pipeMetaMapFromDataNode.get(pipeMetaOnConfigNode.getStaticMeta());
      if (pipeMetaFromDataNode == null) {
        LOGGER.warn(
            "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                + "pipeMetaFromDataNode is null, pipeMetaOnConfigNode: {}",
            pipeMetaOnConfigNode);
        continue;
      }

      final Map<TConsensusGroupId, PipeTaskMeta> pipeTaskMetaMapOnConfigNode =
          pipeMetaOnConfigNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();
      final Map<TConsensusGroupId, PipeTaskMeta> pipeTaskMetaMapFromDataNode =
          pipeMetaFromDataNode.getRuntimeMeta().getConsensusGroupIdToTaskMetaMap();
      for (final Map.Entry<TConsensusGroupId, PipeTaskMeta> runtimeMetaOnConfigNode :
          pipeTaskMetaMapOnConfigNode.entrySet()) {
        if (runtimeMetaOnConfigNode.getValue().getLeaderDataNodeId() != dataNodeId) {
          continue;
        }

        final PipeTaskMeta runtimeMetaFromDataNode =
            pipeTaskMetaMapFromDataNode.get(runtimeMetaOnConfigNode.getKey());
        if (runtimeMetaFromDataNode == null) {
          LOGGER.warn(
              "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, "
                  + "runtimeMetaFromDataNode is null, runtimeMetaOnConfigNode: {}",
              runtimeMetaOnConfigNode);
          continue;
        }

        // update progress index
        if (!runtimeMetaOnConfigNode
            .getValue()
            .getProgressIndex()
            .isAfter(runtimeMetaFromDataNode.getProgressIndex())) {
          runtimeMetaOnConfigNode
              .getValue()
              .updateProgressIndex(runtimeMetaFromDataNode.getProgressIndex());
          needWriteConsensusOnConfigNodes = true;
        }

        // update runtime exception
        final PipeTaskMeta pipeTaskMetaOnConfigNode = runtimeMetaOnConfigNode.getValue();
        pipeTaskMetaOnConfigNode.clearExceptionMessages();
        for (final PipeRuntimeException exception :
            runtimeMetaFromDataNode.getExceptionMessages()) {
          pipeTaskMetaOnConfigNode.trackExceptionMessage(exception);

          if (exception instanceof PipeRuntimeCriticalException) {
            final String pipeName = pipeMetaOnConfigNode.getStaticMeta().getPipeName();
            if (!pipeMetaOnConfigNode
                .getRuntimeMeta()
                .getStatus()
                .get()
                .equals(PipeStatus.STOPPED)) {
              LOGGER.warn(
                  String.format(
                      "Detect PipeRuntimeCriticalException %s from DataNode, stop pipe %s.",
                      exception, pipeName));

              pipeMetaOnConfigNode.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
              needWriteConsensusOnConfigNodes = true;
              needPushPipeMetaToDataNodes = true;
            }

            if (exception instanceof PipeRuntimeConnectorCriticalException) {
              LOGGER.warn(
                  String.format(
                      "Detect PipeRuntimeConnectorCriticalException %s from DataNode.", exception));

              ((PipeTableResp)
                      env.getConfigManager()
                          .getPipeManager()
                          .getPipeTaskCoordinator()
                          .getPipeTaskInfo()
                          .showPipes())
                  .filter(true, pipeName).getAllPipeMeta().stream()
                      .map(pipeMeta -> pipeMeta.getRuntimeMeta().getStatus())
                      .filter(status -> !status.get().equals(PipeStatus.STOPPED))
                      .forEach(
                          status -> {
                            status.set(PipeStatus.STOPPED);
                            needWriteConsensusOnConfigNodes = true;
                            needPushPipeMetaToDataNodes = true;
                          });
            }
          }
        }
      }
    }
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromWriteConfigNodeConsensus");

    if (!needWriteConsensusOnConfigNodes) {
      return;
    }

    final List<PipeMeta> pipeMetaList = new ArrayList<>();
    for (final PipeMeta pipeMeta :
        env.getConfigManager()
            .getPipeManager()
            .getPipeTaskCoordinator()
            .getPipeTaskInfo()
            .getPipeMetaList()) {
      pipeMetaList.add(pipeMeta);
    }

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new PipeHandleMetaChangePlan(pipeMetaList));
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromHandleOnDataNodes");

    if (!needPushPipeMetaToDataNodes) {
      return;
    }

    pushPipeMetaToDataNodes(env);
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromValidateTask");

    // do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromCalculateInfoForTask");

    // do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromWriteConfigNodeConsensus");

    // do nothing
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromOperateOnDataNodes");

    // do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_HANDLE_META_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);

    ReadWriteIOUtils.write(dataNodeId, stream);

    ReadWriteIOUtils.write(pipeMetaByteBufferListFromDataNode.size(), stream);
    for (ByteBuffer pipeMetaByteBuffer : pipeMetaByteBufferListFromDataNode) {
      ReadWriteIOUtils.write(pipeMetaByteBuffer.limit(), stream);
      ReadWriteIOUtils.write(new Binary(pipeMetaByteBuffer.array()), stream);
    }

    ReadWriteIOUtils.write(needWriteConsensusOnConfigNodes, stream);
    ReadWriteIOUtils.write(needPushPipeMetaToDataNodes, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    dataNodeId = ReadWriteIOUtils.readInt(byteBuffer);

    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final int limit = ReadWriteIOUtils.readInt(byteBuffer);
      final ByteBuffer pipeMetaByteBuffer =
          ByteBuffer.wrap(ReadWriteIOUtils.readBinary(byteBuffer).getValues());
      pipeMetaByteBuffer.limit(limit);
      pipeMetaByteBufferListFromDataNode.add(pipeMetaByteBuffer);
    }

    needWriteConsensusOnConfigNodes = ReadWriteIOUtils.readBool(byteBuffer);
    needPushPipeMetaToDataNodes = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PipeHandleMetaChangeProcedure)) {
      return false;
    }
    PipeHandleMetaChangeProcedure that = (PipeHandleMetaChangeProcedure) o;
    return dataNodeId == that.dataNodeId
        && needWriteConsensusOnConfigNodes == that.needWriteConsensusOnConfigNodes
        && needPushPipeMetaToDataNodes == that.needPushPipeMetaToDataNodes
        && Objects.equals(
            pipeMetaByteBufferListFromDataNode, that.pipeMetaByteBufferListFromDataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        dataNodeId,
        pipeMetaByteBufferListFromDataNode,
        needWriteConsensusOnConfigNodes,
        needPushPipeMetaToDataNodes);
  }
}
