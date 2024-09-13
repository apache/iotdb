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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AlterPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterPipeProcedureV2.class);

  private TAlterPipeReq alterPipeRequest;

  private PipeStaticMeta currentPipeStaticMeta;
  private PipeStaticMeta updatedPipeStaticMeta;
  private PipeRuntimeMeta currentPipeRuntimeMeta;
  private PipeRuntimeMeta updatedPipeRuntimeMeta;

  private ProcedureType procedureType;

  public AlterPipeProcedureV2(ProcedureType procedureType) {
    super();
    this.procedureType = procedureType;
  }

  public AlterPipeProcedureV2(TAlterPipeReq alterPipeRequest) throws PipeException {
    super();
    this.alterPipeRequest = alterPipeRequest;
    procedureType = ProcedureType.ALTER_PIPE_PROCEDURE_V3;
  }

  @TestOnly
  public AlterPipeProcedureV2(TAlterPipeReq alterPipeRequest, ProcedureType procedureType)
      throws PipeException {
    super();
    this.alterPipeRequest = alterPipeRequest;
    this.procedureType = procedureType;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.ALTER_PIPE;
  }

  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromValidateTask({})", alterPipeRequest.getPipeName());

    // We should execute checkBeforeAlterPipe before checking the pipe plugin. This method will
    // update the alterPipeRequest based on the alterPipeRequest and existing pipe metadata.
    if (!pipeTaskInfo.get().checkAndUpdateRequestBeforeAlterPipe(alterPipeRequest)) {
      return false;
    }

    final PipeManager pipeManager = env.getConfigManager().getPipeManager();
    pipeManager
        .getPipePluginCoordinator()
        .getPipePluginInfo()
        .checkPipePluginExistence(
            alterPipeRequest.getExtractorAttributes(),
            alterPipeRequest.getProcessorAttributes(),
            alterPipeRequest.getConnectorAttributes());

    return true;
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromCalculateInfoForTask({})",
        alterPipeRequest.getPipeName());

    PipeMeta currentPipeMeta =
        pipeTaskInfo.get().getPipeMetaByPipeName(alterPipeRequest.getPipeName());
    currentPipeStaticMeta = currentPipeMeta.getStaticMeta();
    currentPipeRuntimeMeta = currentPipeMeta.getRuntimeMeta();

    final Map<Integer, PipeTaskMeta> currentConsensusGroupId2PipeTaskMeta =
        currentPipeRuntimeMeta.getConsensusGroupId2TaskMetaMap();

    // Deep copy reused attributes
    updatedPipeStaticMeta =
        new PipeStaticMeta(
            alterPipeRequest.getPipeName(),
            System.currentTimeMillis(),
            new HashMap<>(alterPipeRequest.getExtractorAttributes()),
            new HashMap<>(alterPipeRequest.getProcessorAttributes()),
            new HashMap<>(alterPipeRequest.getConnectorAttributes()));

    final ConcurrentMap<Integer, PipeTaskMeta> updatedConsensusGroupIdToTaskMetaMap =
        new ConcurrentHashMap<>();
    // data regions & schema regions
    env.getConfigManager()
        .getLoadManager()
        .getRegionLeaderMap()
        .forEach(
            (regionGroupId, regionLeaderNodeId) -> {
              final String databaseName =
                  env.getConfigManager().getPartitionManager().getRegionStorageGroup(regionGroupId);
              final PipeTaskMeta currentPipeTaskMeta =
                  currentConsensusGroupId2PipeTaskMeta.get(regionGroupId.getId());
              if (databaseName != null
                  && !databaseName.equals(SchemaConstant.SYSTEM_DATABASE)
                  && !databaseName.startsWith(SchemaConstant.SYSTEM_DATABASE + ".")
                  && currentPipeTaskMeta.getLeaderNodeId() == regionLeaderNodeId) {
                // Pipe only collect user's data, filter metric database here.
                updatedConsensusGroupIdToTaskMetaMap.put(
                    regionGroupId.getId(),
                    new PipeTaskMeta(currentPipeTaskMeta.getProgressIndex(), regionLeaderNodeId));
              }
            });

    final PipeTaskMeta configRegionTaskMeta =
        currentConsensusGroupId2PipeTaskMeta.get(Integer.MIN_VALUE);
    if (Objects.nonNull(configRegionTaskMeta)) {
      // config region
      updatedConsensusGroupIdToTaskMetaMap.put(
          // 0 is the consensus group id of the config region, but data region id and schema region
          // id also start from 0, so we use Integer.MIN_VALUE to represent the config region
          Integer.MIN_VALUE,
          new PipeTaskMeta(
              configRegionTaskMeta.getProgressIndex(),
              // The leader of the config region is the config node itself
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()));
    }

    updatedPipeRuntimeMeta = new PipeRuntimeMeta(updatedConsensusGroupIdToTaskMetaMap);

    // If the pipe's previous status was user stopped, then after the alter operation, the pipe's
    // status remains user stopped; otherwise, it becomes running.
    if (!pipeTaskInfo.get().isPipeStoppedByUser(alterPipeRequest.getPipeName())) {
      updatedPipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
    }
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromWriteConfigNodeConsensus({})",
        alterPipeRequest.getPipeName());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterPipePlanV2(updatedPipeStaticMeta, updatedPipeRuntimeMeta));
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
    final String pipeName = alterPipeRequest.getPipeName();
    LOGGER.info("AlterPipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushSinglePipeMetaToDataNodes(pipeName, env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to alter pipe {}, details: {}, metadata will be synchronized later.",
          alterPipeRequest.getPipeName(),
          exceptionMessage);
    }
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromValidateTask({})", alterPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromCalculateInfoForTask({})",
        alterPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})",
        alterPipeRequest.getPipeName());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterPipePlanV2(currentPipeStaticMeta, currentPipeRuntimeMeta));
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
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromOperateOnDataNodes({})", alterPipeRequest.getPipeName());

    // Push all pipe metas to datanode, may be time-consuming
    String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(
            alterPipeRequest.getPipeName(), pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to rollback alter pipe {}, details: {}, metadata will be synchronized later.",
          alterPipeRequest.getPipeName(),
          exceptionMessage);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(procedureType.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(alterPipeRequest.getPipeName(), stream);
    ReadWriteIOUtils.write(alterPipeRequest.getProcessorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : alterPipeRequest.getProcessorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(alterPipeRequest.getConnectorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : alterPipeRequest.getConnectorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(alterPipeRequest.isReplaceAllProcessorAttributes, stream);
    ReadWriteIOUtils.write(alterPipeRequest.isReplaceAllConnectorAttributes, stream);
    if (currentPipeStaticMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      currentPipeStaticMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    if (updatedPipeStaticMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      updatedPipeStaticMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    if (currentPipeRuntimeMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      currentPipeRuntimeMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    if (updatedPipeRuntimeMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      updatedPipeRuntimeMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (procedureType.getTypeCode() == ProcedureType.ALTER_PIPE_PROCEDURE_V3.getTypeCode()) {
      ReadWriteIOUtils.write(alterPipeRequest.getExtractorAttributesSize(), stream);
      for (Map.Entry<String, String> entry : alterPipeRequest.getExtractorAttributes().entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
      ReadWriteIOUtils.write(alterPipeRequest.isReplaceAllExtractorAttributes, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    alterPipeRequest =
        new TAlterPipeReq()
            .setPipeName(ReadWriteIOUtils.readString(byteBuffer))
            .setExtractorAttributes(new HashMap<>())
            .setProcessorAttributes(new HashMap<>())
            .setConnectorAttributes(new HashMap<>());

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      alterPipeRequest
          .getProcessorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      alterPipeRequest
          .getConnectorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    alterPipeRequest.isReplaceAllProcessorAttributes = ReadWriteIOUtils.readBool(byteBuffer);
    alterPipeRequest.isReplaceAllConnectorAttributes = ReadWriteIOUtils.readBool(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      currentPipeStaticMeta = PipeStaticMeta.deserialize(byteBuffer);
    }
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      updatedPipeStaticMeta = PipeStaticMeta.deserialize(byteBuffer);
    }
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      currentPipeRuntimeMeta = PipeRuntimeMeta.deserialize(byteBuffer);
    }
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      updatedPipeRuntimeMeta = PipeRuntimeMeta.deserialize(byteBuffer);
    }
    if (procedureType.getTypeCode() == ProcedureType.ALTER_PIPE_PROCEDURE_V3.getTypeCode()) {
      size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; ++i) {
        alterPipeRequest
            .getExtractorAttributes()
            .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
      }
      alterPipeRequest.isReplaceAllExtractorAttributes = ReadWriteIOUtils.readBool((byteBuffer));
    } else {
      alterPipeRequest.setExtractorAttributes(new HashMap<>());
      alterPipeRequest.isReplaceAllExtractorAttributes = false;
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
    AlterPipeProcedureV2 that = (AlterPipeProcedureV2) o;
    return this.alterPipeRequest.getPipeName().equals(that.alterPipeRequest.getPipeName())
        && this.alterPipeRequest
            .getExtractorAttributes()
            .toString()
            .equals(that.alterPipeRequest.getExtractorAttributes().toString())
        && this.alterPipeRequest
            .getProcessorAttributes()
            .toString()
            .equals(that.alterPipeRequest.getProcessorAttributes().toString())
        && this.alterPipeRequest
            .getConnectorAttributes()
            .toString()
            .equals(that.alterPipeRequest.getConnectorAttributes().toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        alterPipeRequest.getPipeName(),
        alterPipeRequest.getExtractorAttributes(),
        alterPipeRequest.getProcessorAttributes(),
        alterPipeRequest.getConnectorAttributes());
  }
}
