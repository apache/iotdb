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
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.impl.pipe.util.PipeExternalSourceLoadBalancer;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_EXTRACTOR_PARALLELISM_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_EXTRACTOR_PARALLELISM_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTERNAL_SOURCE_PARALLELISM_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY;

public class CreatePipeProcedureV2 extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeProcedureV2.class);

  private TCreatePipeReq createPipeRequest;

  private PipeStaticMeta pipeStaticMeta;
  private PipeRuntimeMeta pipeRuntimeMeta;

  public CreatePipeProcedureV2() {
    super();
  }

  public CreatePipeProcedureV2(final TCreatePipeReq createPipeRequest) throws PipeException {
    super();
    this.createPipeRequest = createPipeRequest;
  }

  /** This is only used when the pipe task info lock is held by another procedure. */
  public CreatePipeProcedureV2(
      final TCreatePipeReq createPipeRequest, final AtomicReference<PipeTaskInfo> pipeTaskInfo)
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
  public boolean executeFromValidateTask(final ConfigNodeProcedureEnv env) throws PipeException {
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

    checkAndEnrichSourceAuthentication(env, createPipeRequest.getExtractorAttributes());
    checkAndEnrichSinkAuthentication(env, createPipeRequest.getConnectorAttributes());

    return pipeTaskInfo.get().checkBeforeCreatePipe(createPipeRequest);
  }

  public static void checkAndEnrichSourceAuthentication(
      final ConfigNodeProcedureEnv env, final Map<String, String> sourceAttributes) {
    if (Objects.isNull(sourceAttributes)) {
      return;
    }
    final PipeParameters sourceParameters = new PipeParameters(sourceAttributes);

    final String pluginName =
        sourceParameters
            .getStringOrDefault(
                Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName())) {
      return;
    }

    if (sourceParameters.hasAttribute(PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY)
        || sourceParameters.hasAttribute(PipeSourceConstant.SOURCE_IOTDB_USER_KEY)
        || sourceParameters.hasAttribute(PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY)
        || sourceParameters.hasAttribute(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY)) {
      final String hashedPassword =
          env.getConfigManager()
              .getPermissionManager()
              .login4Pipe(
                  sourceParameters.getStringByKeys(
                      PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
                      PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
                      PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
                      PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY),
                  sourceParameters.getStringByKeys(
                      PipeSourceConstant.EXTRACTOR_IOTDB_PASSWORD_KEY,
                      PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
      if (Objects.isNull(hashedPassword)) {
        throw new PipeException("Authentication failed.");
      }
      sourceParameters.addOrReplaceEquivalentAttributes(
          new PipeParameters(
              Collections.singletonMap(
                  PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY, hashedPassword)));
    }
  }

  public static void checkAndEnrichSinkAuthentication(
      final ConfigNodeProcedureEnv env, final Map<String, String> sinkAttributes) {
    final PipeParameters sinkParameters = new PipeParameters(sinkAttributes);

    final String pluginName =
        sinkParameters
            .getStringOrDefault(
                Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName())) {
      return;
    }

    if (sinkParameters.hasAttribute(PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY)
        || sinkParameters.hasAttribute(PipeSinkConstant.SINK_IOTDB_USER_KEY)
        || sinkParameters.hasAttribute(PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY)
        || sinkParameters.hasAttribute(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY)) {
      final String hashedPassword =
          env.getConfigManager()
              .getPermissionManager()
              .login4Pipe(
                  sinkParameters.getStringByKeys(
                      PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY,
                      PipeSinkConstant.SINK_IOTDB_USER_KEY,
                      PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY,
                      PipeSinkConstant.SINK_IOTDB_USERNAME_KEY),
                  sinkParameters.getStringByKeys(
                      PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY,
                      PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY));
      if (Objects.isNull(hashedPassword)) {
        throw new PipeException("Authentication failed.");
      }
      sinkParameters.addOrReplaceEquivalentAttributes(
          new PipeParameters(
              Collections.singletonMap(PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY, hashedPassword)));
    }
  }

  @Override
  public void executeFromCalculateInfoForTask(final ConfigNodeProcedureEnv env) {
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
    } else if (pipeStaticMeta.isSourceExternal()) {
      // external source
      final PipeExternalSourceLoadBalancer loadBalancer =
          new PipeExternalSourceLoadBalancer(
              pipeStaticMeta
                  .getSourceParameters()
                  .getStringOrDefault(
                      Arrays.asList(
                          PipeSourceConstant.EXTERNAL_EXTRACTOR_BALANCE_STRATEGY_KEY,
                          PipeSourceConstant.EXTERNAL_SOURCE_BALANCE_STRATEGY_KEY),
                      PipeSourceConstant.EXTERNAL_EXTRACTOR_BALANCE_PROPORTION_STRATEGY));
      final int parallelism =
          pipeStaticMeta
              .getSourceParameters()
              .getIntOrDefault(
                  Arrays.asList(
                      EXTERNAL_EXTRACTOR_PARALLELISM_KEY, EXTERNAL_SOURCE_PARALLELISM_KEY),
                  EXTERNAL_EXTRACTOR_PARALLELISM_DEFAULT_VALUE);
      loadBalancer
          .balance(parallelism, pipeStaticMeta, env.getConfigManager())
          .forEach(
              (taskIndex, leaderNodeId) -> {
                consensusGroupIdToTaskMetaMap.put(
                    taskIndex, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, leaderNodeId));
              });
    } else {
      // data regions & schema regions
      env.getConfigManager()
          .getLoadManager()
          .getRegionLeaderMap()
          .forEach(
              (regionGroupId, regionLeaderNodeId) -> {
                final String databaseName =
                    env.getConfigManager().getPartitionManager().getRegionDatabase(regionGroupId);
                if (databaseName != null
                    && !databaseName.equals(SchemaConstant.SYSTEM_DATABASE)
                    && !databaseName.startsWith(SchemaConstant.SYSTEM_DATABASE + ".")
                    && !databaseName.equals(SchemaConstant.AUDIT_DATABASE)
                    && !databaseName.startsWith(SchemaConstant.AUDIT_DATABASE + ".")) {
                  // Pipe only collect user's data, filter out metric database here.
                  consensusGroupIdToTaskMetaMap.put(
                      regionGroupId.getId(),
                      new PipeTaskMeta(MinimumProgressIndex.INSTANCE, regionLeaderNodeId));
                }
              });

      // config region
      consensusGroupIdToTaskMetaMap.put(
          // 0 is the consensus group id of the config region, but data region id and schema region
          // id also start from 0, so we use Integer.MIN_VALUE to represent the config region
          Integer.MIN_VALUE,
          new PipeTaskMeta(
              MinimumProgressIndex.INSTANCE,
              // The leader of the config region is the config node itself
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()));
    }

    pipeRuntimeMeta = new PipeRuntimeMeta(consensusGroupIdToTaskMetaMap);
    if (!createPipeRequest.needManuallyStart) {
      pipeRuntimeMeta.getStatus().set(PipeStatus.RUNNING);
    }
  }

  @Override
  public void executeFromWriteConfigNodeConsensus(final ConfigNodeProcedureEnv env)
      throws PipeException {
    LOGGER.info(
        "CreatePipeProcedureV2: executeFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());

    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new CreatePipePlanV2(pipeStaticMeta, pipeRuntimeMeta));
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void executeFromOperateOnDataNodes(final ConfigNodeProcedureEnv env) throws IOException {
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
  public void rollbackFromValidateTask(final ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromValidateTask({})", createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromCalculateInfoForTask(final ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromCalculateInfoForTask({})",
        createPipeRequest.getPipeName());
    // Do nothing
  }

  @Override
  public void rollbackFromWriteConfigNodeConsensus(final ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "CreatePipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})",
        createPipeRequest.getPipeName());
    TSStatus response;
    try {
      response =
          env.getConfigManager()
              .getConsensusManager()
              .write(new DropPipePlanV2(createPipeRequest.getPipeName()));
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      response = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      response.setMessage(e.getMessage());
    }
    if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(response.getMessage());
    }
  }

  @Override
  public void rollbackFromOperateOnDataNodes(final ConfigNodeProcedureEnv env) throws IOException {
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
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(createPipeRequest.getPipeName(), stream);
    ReadWriteIOUtils.write(createPipeRequest.getExtractorAttributesSize(), stream);
    for (final Map.Entry<String, String> entry :
        createPipeRequest.getExtractorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(createPipeRequest.getProcessorAttributesSize(), stream);
    for (final Map.Entry<String, String> entry :
        createPipeRequest.getProcessorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    ReadWriteIOUtils.write(createPipeRequest.getConnectorAttributesSize(), stream);
    for (final Map.Entry<String, String> entry :
        createPipeRequest.getConnectorAttributes().entrySet()) {
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
  public void deserialize(final ByteBuffer byteBuffer) {
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreatePipeProcedureV2 that = (CreatePipeProcedureV2) o;
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
