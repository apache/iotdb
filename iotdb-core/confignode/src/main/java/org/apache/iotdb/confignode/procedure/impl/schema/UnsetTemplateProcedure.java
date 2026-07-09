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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.UnsetTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class UnsetTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, UnsetTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnsetTemplateProcedure.class);

  private String queryId;
  private Template template;
  private PartialPath path;

  private boolean alreadyRollback = false;

  private transient ByteBuffer addTemplateSetInfo;
  private transient ByteBuffer invalidateTemplateSetInfo;

  public UnsetTemplateProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public UnsetTemplateProcedure(
      final String queryId,
      final Template template,
      final PartialPath path,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.template = template;
    this.path = path;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final UnsetTemplateState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info(
              ProcedureMessages.CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TEMPLATE_SET_ON,
              template.getName(),
              path);
          constructBlackList(env);
          break;
        case CLEAN_DATANODE_TEMPLATE_CACHE:
          LOGGER.info(
              ProcedureMessages.INVALIDATE_CACHE_OF_TEMPLATE_SET_ON, template.getName(), path);
          invalidateCache(env);
          break;
        case CHECK_DATANODE_TEMPLATE_ACTIVATION:
          LOGGER.info(
              ProcedureMessages.CHECK_DATANODE_TEMPLATE_ACTIVATION_OF_TEMPLATE_SET_ON,
              template.getName(),
              path);
          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          if (checkDataNodeTemplateActivation(env)) {
            setFailure(new ProcedureException(new TemplateIsInUseException(path.getFullPath())));
            return Flow.NO_MORE_STATE;
          } else {
            setNextState(UnsetTemplateState.UNSET_SCHEMA_TEMPLATE);
          }
          break;
        case UNSET_SCHEMA_TEMPLATE:
          LOGGER.info(ProcedureMessages.UNSET_TEMPLATE_ON, template.getName(), path);
          unsetTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException(ProcedureMessages.UNRECOGNIZED_STATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.UNSETTEMPLATE_COSTS_MS,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void constructBlackList(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .preUnsetSchemaTemplate(template.getId(), path);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UnsetTemplateState.CLEAN_DATANODE_TEMPLATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    try {
      executeInvalidateCache(env);
      setNextState(UnsetTemplateState.CHECK_DATANODE_TEMPLATE_ACTIVATION);
    } catch (final ProcedureException e) {
      setFailure(e);
    }
  }

  private void executeInvalidateCache(final ConfigNodeProcedureEnv env) throws ProcedureException {
    // Proceed once every unreachable DataNode is provably self-fenced (it fails closed on its
    // template cache and resyncs on recovery) instead of hard-failing on the first unreachable one.
    if (!SchemaUtils.broadcastTemplateUpdate(
        env.getConfigManager(),
        () -> {
          final TUpdateTemplateReq invalidateTemplateSetInfoReq = new TUpdateTemplateReq();
          invalidateTemplateSetInfoReq.setType(
              TemplateInternalRPCUpdateType.INVALIDATE_TEMPLATE_SET_INFO.toByte());
          invalidateTemplateSetInfoReq.setTemplateInfo(getInvalidateTemplateSetInfo());
          return invalidateTemplateSetInfoReq;
        })) {
      // all dataNodes must clear the related template cache
      LOGGER.error(
          ProcedureMessages.FAILED_TO_INVALIDATE_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON,
          template.getName(),
          path);
      throw new ProcedureException(
          new MetadataException(ProcedureMessages.INVALIDATE_TEMPLATE_CACHE_FAILED));
    }
  }

  private boolean checkDataNodeTemplateActivation(final ConfigNodeProcedureEnv env) {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(path);
    patternTree.appendPathPattern(path.concatAsMeasurementPath(MULTI_LEVEL_PATH_WILDCARD));
    try {
      return SchemaUtils.checkDataNodeTemplateActivation(
          env.getConfigManager(), patternTree, template);
    } catch (final MetadataException e) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      ProcedureMessages
                          .UNSET_TEMPLATE_FROM_FAILED_WHEN_CHECK_DATANODE_TEMPLATE_ACTIVATION_BECAUSE,
                      template.getName(),
                      path,
                      e.getMessage()))));
      return false;
    }
  }

  private void unsetTemplate(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .unsetSchemaTemplateInBlackList(template.getId(), path, isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final UnsetTemplateState unsetTemplateState)
      throws IOException, InterruptedException, ProcedureException {
    if (alreadyRollback) {
      return;
    }
    alreadyRollback = true;
    ProcedureException rollbackException;
    try {
      executeRollbackInvalidateCache(env);
      final TSStatus status =
          env.getConfigManager()
              .getClusterSchemaManager()
              .rollbackPreUnsetSchemaTemplate(template.getId(), path);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      } else {
        LOGGER.error(
            ProcedureMessages.FAILED_TO_ROLLBACK_PRE_UNSET_TEMPLATE_OPERATION_OF_TEMPLATE_SET,
            template.getName(),
            path);
        rollbackException =
            new ProcedureException(
                new MetadataException(
                    ProcedureMessages.ROLLBACK_TEMPLATE_PRE_UNSET_FAILED_BECAUSE_OF
                        + status.getMessage()));
      }
    } catch (final ProcedureException e) {
      rollbackException = e;
    }
    try {
      executeInvalidateCache(env);
      setFailure(rollbackException);
    } catch (final ProcedureException exception) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  ProcedureMessages
                      .ROLLBACK_UNSET_TEMPLATE_FAILED_AND_THE_CLUSTER_TEMPLATE_INFO_MANAGEMENT)));
    }
  }

  private void executeRollbackInvalidateCache(ConfigNodeProcedureEnv env)
      throws ProcedureException {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    TUpdateTemplateReq rollbackTemplateSetInfoReq = new TUpdateTemplateReq();
    rollbackTemplateSetInfoReq.setType(
        TemplateInternalRPCUpdateType.ROLLBACK_INVALIDATE_TEMPLATE_SET_INFO.toByte());
    rollbackTemplateSetInfoReq.setTemplateInfo(getAddTemplateSetInfo());
    DataNodeAsyncRequestContext<TUpdateTemplateReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TEMPLATE,
            rollbackTemplateSetInfoReq,
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related template cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            ProcedureMessages.FAILED_TO_ROLLBACK_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON,
            template.getName(),
            path);
        throw new ProcedureException(
            new MetadataException(ProcedureMessages.ROLLBACK_TEMPLATE_CACHE_FAILED));
      }
    }
  }

  private ByteBuffer getAddTemplateSetInfo() {
    if (this.addTemplateSetInfo == null) {
      this.addTemplateSetInfo =
          ByteBuffer.wrap(
              TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(
                  template, path.getFullPath()));
    }

    return addTemplateSetInfo;
  }

  @Override
  protected boolean isRollbackSupported(final UnsetTemplateState unsetTemplateState) {
    return true;
  }

  @Override
  protected UnsetTemplateState getState(final int stateId) {
    return UnsetTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(final UnsetTemplateState unsetTemplateState) {
    return unsetTemplateState.ordinal();
  }

  @Override
  protected UnsetTemplateState getInitialState() {
    return UnsetTemplateState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getTemplateId() {
    return template.getId();
  }

  public String getTemplateName() {
    return template.getName();
  }

  public Template getTemplate() {
    return template;
  }

  public PartialPath getPath() {
    return path;
  }

  private ByteBuffer getInvalidateTemplateSetInfo() {
    if (this.invalidateTemplateSetInfo == null) {
      this.invalidateTemplateSetInfo =
          ByteBuffer.wrap(
              TemplateInternalRPCUtil.generateInvalidateTemplateSetInfoBytes(
                  template.getId(), path.getFullPath()));
    }
    return this.invalidateTemplateSetInfo;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_UNSET_TEMPLATE_PROCEDURE.getTypeCode()
            : ProcedureType.UNSET_TEMPLATE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    template.serialize(stream);
    path.serialize(stream);
    ReadWriteIOUtils.write(alreadyRollback, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    template = new Template();
    template.deserialize(byteBuffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    alreadyRollback = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final UnsetTemplateProcedure that = (UnsetTemplateProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && Objects.equals(getCycles(), that.getCycles())
        && Objects.equals(isGeneratedByPipe, that.isGeneratedByPipe)
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(template, that.template)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, queryId, template, path);
  }
}
