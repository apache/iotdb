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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeType;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.PipePlugin;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY;

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

  /** This is only used when the pipe task info lock is held by another procedure. */
  public CreatePipeProcedureV2(
      TCreatePipeReq createPipeRequest, AtomicReference<PipeTaskInfo> pipeTaskInfo)
      throws PipeException {
    super();
    this.pipeTaskInfo = pipeTaskInfo;
    this.createPipeRequest = createPipeRequest;
  }

  /**
   * This should be called after {@link #executeFromValidateTask} and {@link
   * #executeFromCalculateInfoForTask}.
   */
  public String getPipeName() {
    return createPipeRequest.getPipeName();
  }

  /**
   * This should be called after {@link #executeFromValidateTask} and {@link
   * #executeFromCalculateInfoForTask}.
   */
  public CreatePipePlanV2 constructPlan() {
    return new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta);
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.CREATE_PIPE;
  }

  /**
   * Check the {@link PipePlugin} configuration in Pipe. If there is an error, throw {@link
   * PipeException}. If there is a Pipe with the same name and there is no IfNotExists condition in
   * {@link #createPipeRequest}, throw {@link PipeException}. If there is an IfNotExists condition,
   * return {@code false}. If there is no Pipe with the same name, return {@code true}.
   *
   * @param env the environment for the procedure
   * @return {@code true} The pipeline does not exist {@code false} The pipeline already exists and
   *     satisfies the IfNotExists condition
   * @throws PipeException
   */
  @Override
  public boolean executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromValidateTask({})", createPipeRequest.getPipeName());

    final PipeManager pipeManager = env.getConfigManager().getPipeManager();
    pipeManager
        .getPipePluginCoordinator()
        .getPipePluginInfo()
        .checkPipePluginExistence(
            createPipeRequest.getExtractorAttributes(),
            createPipeRequest.getProcessorAttributes(),
            createPipeRequest.getConnectorAttributes());

    return pipeTaskInfo.get().checkBeforeCreatePipe(createPipeRequest);
  }

  @Override
  public void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
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

    final ConcurrentMap<Integer, PipeTaskMeta> consensusGroupIdToTaskMetaMap =
        new ConcurrentHashMap<>();

    if (PipeType.CONSENSUS.equals(pipeStaticMeta.getPipeType())) {
      final TConsensusGroupId groupId =
          ConsensusGroupId.Factory.createFromString(
                  createPipeRequest.getExtractorAttributes().get(EXTRACTOR_CONSENSUS_GROUP_ID_KEY))
              .convertToTConsensusGroupId();

      final int senderDataNodeId =
          Integer.parseInt(
              createPipeRequest
                  .getExtractorAttributes()
                  .get(EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY));
      consensusGroupIdToTaskMetaMap.put(
          groupId.getId(),
          new PipeTaskMeta(
              new RecoverProgressIndex(senderDataNodeId, new SimpleProgressIndex(0, 0)),
              senderDataNodeId));
    } else {
      // data regions & schema regions
      env.getConfigManager()
          .getLoadManager()
          .getRegionLeaderMap()
          .forEach(
              (regionGroupId, regionLeaderNodeId) -> {
                final String databaseName =
                    env.getConfigManager()
                        .getPartitionManager()
                        .getRegionStorageGroup(regionGroupId);
                if (databaseName != null
                    && !databaseName.equals(SchemaConstant.SYSTEM_DATABASE)
                    && !databaseName.startsWith(SchemaConstant.SYSTEM_DATABASE + ".")) {
                  // Pipe only collect user's data, filter out metric database here.
                  consensusGroupIdToTaskMetaMap.put(
                      regionGroupId.getId(),
                      new PipeTaskMeta(MinimumProgressIndex.INSTANCE, regionLeaderNodeId));
                }
              });

      // config region
      consensusGroupIdToTaskMetaMap.put(
          // 0 is the consensus group id of the config region, but data region id and schema region
          // id
          // also start from 0, so we use Integer.MIN_VALUE to represent the config region
          Integer.MIN_VALUE,
          new PipeTaskMeta(
              MinimumProgressIndex.INSTANCE,
              // The leader of the config region is the config node itself
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()));
    }

    pipeRuntimeMeta = new PipeRuntimeMeta(consensusGroupIdToTaskMetaMap);
    pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));
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
    final String pipeName = createPipeRequest.getPipeName();
    LOGGER.info("CreatePipeProcedureV2: executeFromOperateOnDataNodes({})", pipeName);

    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(pipeName, pushSinglePipeMetaToDataNodes(pipeName, env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to create pipe {}, details: {}, metadata will be synchronized later.",
          createPipeRequest.getPipeName(),
          exceptionMessage);
    }
  }

  @Override
  public void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromValidateTask({})", createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromCalculateInfoForTask({})",
        createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropPipePlanV2(createPipeRequest.getPipeName()));
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
        "CreatePipeProcedureV2: rollbackFromOperateOnDataNodes({})",
        createPipeRequest.getPipeName());

    // Push all pipe metas to datanode, may be time-consuming
    final String exceptionMessage =
        parsePushPipeMetaExceptionForPipe(
            createPipeRequest.getPipeName(), pushPipeMetaToDataNodes(env));
    if (!exceptionMessage.isEmpty()) {
      LOGGER.warn(
          "Failed to rollback create pipe {}, details: {}, metadata will be synchronized later.",
          createPipeRequest.getPipeName(),
          exceptionMessage);
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
    return this.createPipeRequest.getPipeName().equals(that.createPipeRequest.getPipeName())
        && this.createPipeRequest
            .getExtractorAttributes()
            .toString()
            .equals(that.createPipeRequest.getExtractorAttributes().toString())
        && this.createPipeRequest
            .getProcessorAttributes()
            .toString()
            .equals(that.createPipeRequest.getProcessorAttributes().toString())
        && this.createPipeRequest
            .getConnectorAttributes()
            .toString()
            .equals(that.createPipeRequest.getConnectorAttributes().toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        createPipeRequest.getPipeName(),
        createPipeRequest.getExtractorAttributes(),
        createPipeRequest.getProcessorAttributes(),
        createPipeRequest.getConnectorAttributes());
  }
}
