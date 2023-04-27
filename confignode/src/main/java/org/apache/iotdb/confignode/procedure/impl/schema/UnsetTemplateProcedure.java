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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.UnsetTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.exception.metadata.template.TemplateIsInUseException;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUtil;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public UnsetTemplateProcedure() {
    super();
  }

  public UnsetTemplateProcedure(String queryId, Template template, PartialPath path) {
    super();
    this.queryId = queryId;
    this.template = template;
    this.path = path;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, UnsetTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info(
              "Construct schema black list of template {} set on {}", template.getName(), path);
          constructBlackList(env);
          break;
        case CLEAN_DATANODE_TEMPLATE_CACHE:
          LOGGER.info("Invalidate cache of template {} set on {}", template.getName(), path);
          invalidateCache(env);
          break;
        case CHECK_DATANODE_TEMPLATE_ACTIVATION:
          LOGGER.info(
              "Check DataNode template activation of template {} set on {}",
              template.getName(),
              path);
          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          if (checkDataNodeTemplateActivation(env) > 0) {
            setFailure(new ProcedureException(new TemplateIsInUseException(path.getFullPath())));
            return Flow.NO_MORE_STATE;
          } else {
            setNextState(UnsetTemplateState.UNSET_SCHEMA_TEMPLATE);
          }
          break;
        case UNSET_SCHEMA_TEMPLATE:
          LOGGER.info("Unset template {} on {}", template.getName(), path);
          unsetTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "UnsetTemplate-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  private void constructBlackList(ConfigNodeProcedureEnv env) {
    TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .preUnsetSchemaTemplate(template.getId(), path);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UnsetTemplateState.CLEAN_DATANODE_TEMPLATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    try {
      executeInvalidateCache(env);
      setNextState(UnsetTemplateState.CHECK_DATANODE_TEMPLATE_ACTIVATION);
    } catch (ProcedureException e) {
      setFailure(e);
    }
  }

  private void executeInvalidateCache(ConfigNodeProcedureEnv env) throws ProcedureException {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    TUpdateTemplateReq invalidateTemplateSetInfoReq = new TUpdateTemplateReq();
    invalidateTemplateSetInfoReq.setType(
        TemplateInternalRPCUpdateType.INVALIDATE_TEMPLATE_SET_INFO.toByte());
    invalidateTemplateSetInfoReq.setTemplateInfo(getInvalidateTemplateSetInfo());
    AsyncClientHandler<TUpdateTemplateReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_TEMPLATE, invalidateTemplateSetInfoReq, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related template cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to invalidate template cache of template {} set on {}",
            template.getName(),
            path);
        throw new ProcedureException(new MetadataException("Invalidate template cache failed"));
      }
    }
  }

  private long checkDataNodeTemplateActivation(ConfigNodeProcedureEnv env) {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(path);
    patternTree.appendPathPattern(path.concatNode(MULTI_LEVEL_PATH_WILDCARD));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {
    }
    ByteBuffer patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);

    List<TCountPathsUsingTemplateResp> respList = new ArrayList<>();
    DataNodeRegionTaskExecutor<TCountPathsUsingTemplateReq, TCountPathsUsingTemplateResp>
        regionTask =
            new DataNodeRegionTaskExecutor<
                TCountPathsUsingTemplateReq, TCountPathsUsingTemplateResp>(
                env,
                relatedSchemaRegionGroup,
                false,
                DataNodeRequestType.COUNT_PATHS_USING_TEMPLATE,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TCountPathsUsingTemplateReq(
                        template.getId(), patternTreeBytes, consensusGroupIdList))) {

              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  TDataNodeLocation dataNodeLocation,
                  List<TConsensusGroupId> consensusGroupIdList,
                  TCountPathsUsingTemplateResp response) {
                respList.add(response);
                List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  return failedRegionList;
                }

                if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  List<TSStatus> subStatus = response.getStatus().getSubStatus();
                  for (int i = 0; i < subStatus.size(); i++) {
                    if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                      failedRegionList.add(consensusGroupIdList.get(i));
                    }
                  }
                } else {
                  failedRegionList.addAll(consensusGroupIdList);
                }
                return failedRegionList;
              }

              @Override
              protected void onAllReplicasetFailure(
                  TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet) {
                setFailure(
                    new ProcedureException(
                        new MetadataException(
                            String.format(
                                "Unset template %s from %s failed when [check DataNode template activation] because all replicaset of schemaRegion %s failed. %s",
                                template.getName(),
                                path,
                                consensusGroupId.id,
                                dataNodeLocationSet))));
                interruptTask();
              }
            };
    regionTask.execute();
    if (isFailed()) {
      return 0;
    }

    long result = 0;
    for (TCountPathsUsingTemplateResp resp : respList) {
      result += resp.getCount();
    }

    return result;
  }

  private void unsetTemplate(ConfigNodeProcedureEnv env) {
    TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .unsetSchemaTemplateInBlackList(template.getId(), path);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UnsetTemplateState.CLEAN_DATANODE_TEMPLATE_CACHE);
    } else {
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, UnsetTemplateState unsetTemplateState)
      throws IOException, InterruptedException, ProcedureException {
    if (alreadyRollback) {
      return;
    }
    alreadyRollback = true;
    ProcedureException rollbackException = null;
    try {
      executeRollbackInvalidateCache(env);
      TSStatus status =
          env.getConfigManager()
              .getClusterSchemaManager()
              .rollbackPreUnsetSchemaTemplate(template.getId(), path);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return;
      } else {
        LOGGER.error(
            "Failed to rollback pre unset template operation of template {} set on {}",
            template.getName(),
            path);
        rollbackException =
            new ProcedureException(
                new MetadataException(
                    "Rollback template pre unset failed because of" + status.getMessage()));
      }
    } catch (ProcedureException e) {
      rollbackException = e;
    }
    try {
      executeInvalidateCache(env);
      setFailure(rollbackException);
    } catch (ProcedureException exception) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  "Rollback unset template failed and the cluster template info management is strictly broken. Please try unset again.")));
    }
  }

  private void executeRollbackInvalidateCache(ConfigNodeProcedureEnv env)
      throws ProcedureException {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    TUpdateTemplateReq rollbackTemplateSetInfoReq = new TUpdateTemplateReq();
    rollbackTemplateSetInfoReq.setType(
        TemplateInternalRPCUpdateType.ADD_TEMPLATE_SET_INFO.toByte());
    rollbackTemplateSetInfoReq.setTemplateInfo(getAddTemplateSetInfo());
    AsyncClientHandler<TUpdateTemplateReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_TEMPLATE, rollbackTemplateSetInfoReq, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related template cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to rollback template cache of template {} set on {}", template.getName(), path);
        throw new ProcedureException(new MetadataException("Rollback template cache failed"));
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(UnsetTemplateState unsetTemplateState) {
    return true;
  }

  @Override
  protected UnsetTemplateState getState(int stateId) {
    return UnsetTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(UnsetTemplateState unsetTemplateState) {
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

  private ByteBuffer getAddTemplateSetInfo() {
    if (this.addTemplateSetInfo == null) {
      this.addTemplateSetInfo =
          ByteBuffer.wrap(
              TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(
                  template, path.getFullPath()));
    }

    return addTemplateSetInfo;
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
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.UNSET_TEMPLATE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    template.serialize(stream);
    path.serialize(stream);
    ReadWriteIOUtils.write(alreadyRollback, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    template = new Template();
    template.deserialize(byteBuffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    alreadyRollback = ReadWriteIOUtils.readBool(byteBuffer);
  }
}
