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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.PipeManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.db.conf.IoTDBConfig;
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

public class CreatePipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeProcedureV2.class);

  private TCreatePipeReq createPipeRequest;

  private PipeStaticMeta pipeStaticMeta;
  private PipeRuntimeMeta pipeRuntimeMeta;

  public CreatePipeProcedureV2() {
    super();
  }

  public CreatePipeProcedureV2(TCreatePipeReq createPipeRequest) throws PipeException {
    super();
    this.createPipeRequest = createPipeRequest;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_PIPE;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromValidateTask({})", createPipeRequest.getPipeName());

    final PipeManager pipeManager = env.getConfigManager().getPipeManager();
    pipeManager
        .getPipePluginCoordinator()
        .getPipePluginInfo()
        .checkBeforeCreatePipe(createPipeRequest);
    pipeManager.getPipeTaskCoordinator().getPipeTaskInfo().checkBeforeCreatePipe(createPipeRequest);
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromCalculateInfoForTask({})",
        createPipeRequest.getPipeName());

    pipeStaticMeta =
        new PipeStaticMeta(
            createPipeRequest.getPipeName(),
            System.currentTimeMillis(),
            createPipeRequest.getExtractorAttributes(),
            createPipeRequest.getProcessorAttributes(),
            createPipeRequest.getConnectorAttributes());

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
                if (databaseName != null && !databaseName.equals(IoTDBConfig.SYSTEM_DATABASE)) {
                  // Pipe only collect user's data, filter metric database here.
                  consensusGroupIdToTaskMetaMap.put(
                      regionGroupId,
                      new PipeTaskMeta(new MinimumProgressIndex(), regionLeaderNodeId));
                }
              }
            });
    pipeRuntimeMeta = new PipeRuntimeMeta(consensusGroupIdToTaskMetaMap);
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env)
      throws PipeException, IOException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromOperateOnDataNodes({})",
        createPipeRequest.getPipeName());

    TSStatus result = pushPipeMetaToDataNodes(env);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Failed to create pipe %s on data nodes. Failures: %s",
              pipeStaticMeta.getPipeName(), result.getMessage()));
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromValidateTask({})", createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromCalculateInfoForTask({})",
        createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());

    final ConsensusWriteResponse response =
        env.getConfigManager()
            .getConsensusManager()
            .write(new DropPipePlanV2(createPipeRequest.getPipeName()));
    if (!response.isSuccessful()) {
      throw new PipeException(response.getErrorMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromOperateOnDataNodes({})",
        createPipeRequest.getPipeName());

    TSStatus result = pushPipeMetaToDataNodes(env);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Failed to rollback create pipe %s on data nodes. Failures: %s",
              pipeStaticMeta.getPipeName(), result.getMessage()));
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(createPipeRequest.getPipeName(), stream);
    ReadWriteIOUtils.write(createPipeRequest.getExtractorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : createPipeRequest.getExtractorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(createPipeRequest.getProcessorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : createPipeRequest.getProcessorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(createPipeRequest.getConnectorAttributesSize(), stream);
    for (Map.Entry<String, String> entry : createPipeRequest.getConnectorAttributes().entrySet()) {
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
    createPipeRequest =
        new TCreatePipeReq()
            .setPipeName(ReadWriteIOUtils.readString(byteBuffer))
            .setExtractorAttributes(new HashMap<>())
            .setProcessorAttributes(new HashMap<>())
            .setConnectorAttributes(new HashMap<>());
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      createPipeRequest
          .getExtractorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      createPipeRequest
          .getProcessorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      createPipeRequest
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
    CreatePipeProcedureV2 that = (CreatePipeProcedureV2) o;
    return createPipeRequest.getPipeName().equals(that.createPipeRequest.getPipeName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(createPipeRequest.getPipeName());
  }
}
