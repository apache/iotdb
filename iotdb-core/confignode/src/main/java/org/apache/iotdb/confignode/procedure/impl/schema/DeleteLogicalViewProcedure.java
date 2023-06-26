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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteLogicalViewState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.exception.metadata.view.ViewNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TConstructViewSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteViewSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackViewSchemaBlackListReq;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DeleteLogicalViewProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteLogicalViewState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteLogicalViewProcedure.class);

  private String queryId;

  private PathPatternTree patternTree;
  private transient ByteBuffer patternTreeBytes;

  private transient String requestMessage;

  public DeleteLogicalViewProcedure() {
    super();
  }

  public DeleteLogicalViewProcedure(String queryId, PathPatternTree patternTree) {
    super();
    this.queryId = queryId;
    setPatternTree(patternTree);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteLogicalViewState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct view schemaengine black list of view {}", requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeleteLogicalViewState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            setFailure(
                new ProcedureException(
                    new ViewNotExistException(
                        patternTree.getAllPathPatterns().stream()
                            .map(PartialPath::getFullPath)
                            .collect(Collectors.toList()))));
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of view {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_VIEW_SCHEMA:
          LOGGER.info("Delete view schemaengine of {}", requestMessage);
          deleteViewSchema(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "DeleteLogicalView-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  // return the total num of timeseries in schemaengine black list
  private long constructBlackList(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    List<TSStatus> successResult = new ArrayList<>();
    DeleteLogicalViewRegionTaskExecutor<TConstructViewSchemaBlackListReq> constructBlackListTask =
        new DeleteLogicalViewRegionTaskExecutor<TConstructViewSchemaBlackListReq>(
            "construct view schemaengine black list",
            env,
            targetSchemaRegionGroup,
            DataNodeRequestType.CONSTRUCT_VIEW_SCHEMA_BLACK_LIST,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TConstructViewSchemaBlackListReq(consensusGroupIdList, patternTreeBytes))) {
          @Override
          protected List<TConsensusGroupId> processResponseOfOneDataNode(
              TDataNodeLocation dataNodeLocation,
              List<TConsensusGroupId> consensusGroupIdList,
              TSStatus response) {
            List<TConsensusGroupId> failedRegionList = new ArrayList<>();
            if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              successResult.add(response);
            } else if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
              List<TSStatus> subStatusList = response.getSubStatus();
              for (int i = 0; i < subStatusList.size(); i++) {
                if (subStatusList.get(i).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  successResult.add(subStatusList.get(i));
                } else {
                  failedRegionList.add(consensusGroupIdList.get(i));
                }
              }
            } else {
              failedRegionList.addAll(consensusGroupIdList);
            }
            return failedRegionList;
          }
        };
    constructBlackListTask.execute();

    if (isFailed()) {
      return 0;
    }

    long preDeletedNum = 0;
    for (TSStatus resp : successResult) {
      preDeletedNum += Long.parseLong(resp.getMessage());
    }
    return preDeletedNum;
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(patternTreeBytes),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schemaengine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schemaengine cache of view {}", requestMessage);
        setFailure(
            new ProcedureException(new MetadataException("Invalidate view schemaengine cache failed")));
        return;
      }
    }

    setNextState(DeleteLogicalViewState.DELETE_VIEW_SCHEMA);
  }

  private void deleteViewSchema(ConfigNodeProcedureEnv env) {
    DeleteLogicalViewRegionTaskExecutor<TDeleteViewSchemaReq> deleteTimeSeriesTask =
        new DeleteLogicalViewRegionTaskExecutor<>(
            "delete view schemaengine",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
            DataNodeRequestType.DELETE_VIEW,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteViewSchemaReq(consensusGroupIdList, patternTreeBytes)));
    deleteTimeSeriesTask.execute();
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeleteLogicalViewState deleteLogicalViewState)
      throws IOException, InterruptedException, ProcedureException {
    DeleteLogicalViewRegionTaskExecutor<TRollbackViewSchemaBlackListReq> rollbackStateTask =
        new DeleteLogicalViewRegionTaskExecutor<>(
            "roll back view schemaengine black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
            DataNodeRequestType.ROLLBACK_VIEW_SCHEMA_BLACK_LIST,
            (dataNodeLocation, consensusGroupIdList) ->
                new TRollbackViewSchemaBlackListReq(consensusGroupIdList, patternTreeBytes));
    rollbackStateTask.execute();
  }

  @Override
  protected boolean isRollbackSupported(DeleteLogicalViewState deleteLogicalViewState) {
    return true;
  }

  @Override
  protected DeleteLogicalViewState getState(int stateId) {
    return DeleteLogicalViewState.values()[stateId];
  }

  @Override
  protected int getStateId(DeleteLogicalViewState deleteLogicalViewState) {
    return deleteLogicalViewState.ordinal();
  }

  @Override
  protected DeleteLogicalViewState getInitialState() {
    return DeleteLogicalViewState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public void setPatternTree(PathPatternTree patternTree) {
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    patternTreeBytes = preparePatternTreeBytesData(patternTree);
  }

  private ByteBuffer preparePatternTreeBytesData(PathPatternTree patternTree) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {

    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    patternTree.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteLogicalViewProcedure that = (DeleteLogicalViewProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getState() == that.getState()
        && patternTree.equals(that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), patternTree);
  }

  private class DeleteLogicalViewRegionTaskExecutor<Q>
      extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    DeleteLogicalViewRegionTaskExecutor(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        DataNodeRequestType dataNodeRequestType,
        BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetSchemaRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        TDataNodeLocation dataNodeLocation,
        List<TConsensusGroupId> consensusGroupIdList,
        TSStatus response) {
      List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        List<TSStatus> subStatus = response.getSubStatus();
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
                      "Delete view %s failed when [%s] because all replicaset of schemaRegion %s failed. %s",
                      requestMessage, taskName, consensusGroupId.id, dataNodeLocationSet))));
      interruptTask();
    }
  }
}
