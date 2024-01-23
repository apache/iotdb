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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AlterPipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterPipeProcedureV2.class);

  private TAlterPipeReq alterPipeRequest;

  private PipeStaticMeta pipeStaticMeta;
  private PipeRuntimeMeta pipeRuntimeMeta;

  public AlterPipeProcedureV2() {
    super();
  }

  public AlterPipeProcedureV2(TAlterPipeReq alterPipeRequest) throws PipeException {
    super();
    this.alterPipeRequest = alterPipeRequest;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_PIPE;
  }

  @Override
  protected boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromValidateTask({})", alterPipeRequest.getPipeName());

    final PipeManager pipeManager = env.getConfigManager().getPipeManager();
    pipeManager
        .getPipePluginCoordinator()
        .getPipePluginInfo()
        .checkPipePluginExistence(
            alterPipeRequest.getExtractorAttributes(),
            alterPipeRequest.getProcessorAttributes(),
            alterPipeRequest.getConnectorAttributes());
    pipeTaskInfo.get().checkBeforeAlterPipe(alterPipeRequest);

    return false;
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromCalculateInfoForTask({})",
        alterPipeRequest.getPipeName());

    pipeStaticMeta =
        new PipeStaticMeta(
            alterPipeRequest.getPipeName(),
            System.currentTimeMillis(),
            alterPipeRequest.getExtractorAttributes(),
            alterPipeRequest.getProcessorAttributes(),
            alterPipeRequest.getConnectorAttributes());

    final Map<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToTaskMetaMap = new HashMap<>();
    env.getConfigManager()
        .getLoadManager()
        .getRegionLeaderMap()
        .forEach(
            (regionGroupId, regionLeaderNodeId) -> {
              if (regionGroupId.getType().equals(TConsensusGroupType.DataRegion)) {
                final String databaseName =
                    env.getConfigManager()
                        .getPartitionManager()
                        .getRegionStorageGroup(regionGroupId);
                if (databaseName != null && !databaseName.equals(SchemaConstant.SYSTEM_DATABASE)) {
                  // Pipe only collect user's data, filter metric database here.
                  consensusGroupIdToTaskMetaMap.put(
                      regionGroupId,
                      new PipeTaskMeta(MinimumProgressIndex.INSTANCE, regionLeaderNodeId));
                }
              }
            });
    pipeRuntimeMeta = new PipeRuntimeMeta(consensusGroupIdToTaskMetaMap);
    pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException {
    LOGGER.info(
        "AlterPipeProcedureV2: executeFromWriteConfigNodeConsensus({})",
        alterPipeRequest.getPipeName());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new AlterPipePlanV2(pipeStaticMeta, pipeRuntimeMeta));
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
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
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
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromValidateTask({})", alterPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromCalculateInfoForTask({})",
        alterPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "AlterPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})",
        alterPipeRequest.getPipeName());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropPipePlanV2(alterPipeRequest.getPipeName()));
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
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
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
    stream.writeShort(ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(alterPipeRequest.getPipeName(), stream);
    ReadWriteIOUtils.write(alterPipeRequest.getExtractorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : alterPipeRequest.getExtractorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
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
    if (pipeStaticMeta != null) {
      ReadWriteIOUtils.write(true, stream);
      pipeStaticMeta.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
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
          .getExtractorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
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
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      pipeStaticMeta = PipeStaticMeta.deserialize(byteBuffer);
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
    return alterPipeRequest.getPipeName().equals(that.alterPipeRequest.getPipeName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(alterPipeRequest.getPipeName());
  }
}
