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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterLogicalViewState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.metadata.view.ViewNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TAlterViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class AlterLogicalViewProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterLogicalViewState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterLogicalViewProcedure.class);

  private String queryId;

  private Map<PartialPath, ViewExpression> viewPathToSourceMap;

  private transient PathPatternTree pathPatternTree;
  private transient ByteBuffer patternTreeBytes;

  public AlterLogicalViewProcedure(boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterLogicalViewProcedure(
      String queryId,
      Map<PartialPath, ViewExpression> viewPathToSourceMap,
      boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.viewPathToSourceMap = viewPathToSourceMap;
    generatePathPatternTree();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AlterLogicalViewState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of view {}", viewPathToSourceMap.keySet());
          invalidateCache(env);
          setNextState(AlterLogicalViewState.ALTER_LOGICAL_VIEW);
          return Flow.HAS_MORE_STATE;
        case ALTER_LOGICAL_VIEW:
          LOGGER.info("Alter view {}", viewPathToSourceMap.keySet());
          try {
            alterLogicalView(env);
          } catch (ProcedureException e) {
            setFailure(e);
          }
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
    } finally {
      LOGGER.info(
          "AlterLogicalView-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(patternTreeBytes),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schemaengine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to invalidate schemaengine cache of view {}", viewPathToSourceMap.keySet());
        setFailure(
            new ProcedureException(
                new MetadataException("Invalidate view schemaengine cache failed")));
        return;
      }
    }
  }

  private void alterLogicalView(ConfigNodeProcedureEnv env) throws ProcedureException {
    Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(pathPatternTree);
    Map<TConsensusGroupId, Map<PartialPath, ViewExpression>> schemaRegionRequestMap =
        new HashMap<>();
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      schemaRegionRequestMap
          .computeIfAbsent(getBelongedSchemaRegion(env, entry.getKey()), k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }
    AlterLogicalViewRegionTaskExecutor<TAlterViewReq> regionTaskExecutor =
        new AlterLogicalViewRegionTaskExecutor<>(
            "Alter view",
            env,
            targetSchemaRegionGroup,
            CnToDnRequestType.ALTER_VIEW,
            (dataNodeLocation, consensusGroupIdList) -> {
              TAlterViewReq req = new TAlterViewReq().setIsGeneratedByPipe(isGeneratedByPipe);
              req.setSchemaRegionIdList(consensusGroupIdList);
              List<ByteBuffer> viewMapBinaryList = new ArrayList<>();
              for (TConsensusGroupId consensusGroupId : consensusGroupIdList) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                Map<PartialPath, ViewExpression> viewMap =
                    schemaRegionRequestMap.get(consensusGroupId);
                try {
                  ReadWriteIOUtils.write(viewMap.size(), stream);
                  for (Map.Entry<PartialPath, ViewExpression> viewEntry : viewMap.entrySet()) {
                    viewEntry.getKey().serialize(stream);
                    ViewExpression.serialize(viewEntry.getValue(), stream);
                  }
                } catch (IOException e) {
                  // won't reach here
                }
                viewMapBinaryList.add(ByteBuffer.wrap(stream.toByteArray()));
              }
              req.setViewBinaryList(viewMapBinaryList);
              return req;
            });
    regionTaskExecutor.execute();
    if (isFailed()) {
      return;
    }

    invalidateCache(env);
  }

  private TConsensusGroupId getBelongedSchemaRegion(
      ConfigNodeProcedureEnv env, PartialPath viewPath) throws ProcedureException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(viewPath);
    patternTree.constructTree();
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        env.getConfigManager().getSchemaPartition(patternTree).schemaPartitionTable;
    if (schemaPartitionTable.isEmpty()) {
      throw new ProcedureException(new ViewNotExistException(viewPath.getFullPath()));
    } else {
      Map<TSeriesPartitionSlot, TConsensusGroupId> slotMap =
          schemaPartitionTable.values().iterator().next();
      if (slotMap.isEmpty()) {
        throw new ProcedureException(new ViewNotExistException(viewPath.getFullPath()));
      } else {
        return slotMap.values().iterator().next();
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(AlterLogicalViewState alterLogicalViewState) {
    return true;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, AlterLogicalViewState alterLogicalViewState)
      throws IOException, InterruptedException, ProcedureException {
    invalidateCache(env);
  }

  @Override
  protected AlterLogicalViewState getState(int stateId) {
    return AlterLogicalViewState.values()[stateId];
  }

  @Override
  protected int getStateId(AlterLogicalViewState alterLogicalViewState) {
    return alterLogicalViewState.ordinal();
  }

  @Override
  protected AlterLogicalViewState getInitialState() {
    return AlterLogicalViewState.CLEAN_DATANODE_SCHEMA_CACHE;
  }

  public String getQueryId() {
    return queryId;
  }

  private void generatePathPatternTree() {
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : viewPathToSourceMap.keySet()) {
      patternTree.appendFullPath(path);
    }
    patternTree.constructTree();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // won't reach here
    }

    this.pathPatternTree = patternTree;
    this.patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), stream);
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(stream);
      ViewExpression.serialize(entry.getValue(), stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);

    Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }
    this.viewPathToSourceMap = viewPathToSourceMap;
    generatePathPatternTree();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AlterLogicalViewProcedure)) return false;
    AlterLogicalViewProcedure that = (AlterLogicalViewProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && Objects.equals(getCycles(), that.getCycles())
        && Objects.equals(isGeneratedByPipe, that.isGeneratedByPipe)
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(viewPathToSourceMap, that.viewPathToSourceMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        queryId,
        viewPathToSourceMap);
  }

  private class AlterLogicalViewRegionTaskExecutor<Q>
      extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    private final List<TSStatus> failureStatusList = new ArrayList<>();

    AlterLogicalViewRegionTaskExecutor(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        CnToDnRequestType dataNodeRequestType,
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
        List<TSStatus> subStatusList = response.getSubStatus();
        TSStatus subStatus;
        for (int i = 0; i < subStatusList.size(); i++) {
          subStatus = subStatusList.get(i);
          if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            if (subStatus.getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
              failedRegionList.add(consensusGroupIdList.get(i));
            } else {
              collectFailure(subStatus);
              interruptTask();
            }
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    private void collectFailure(TSStatus failureStatus) {
      if (failureStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        failureStatusList.addAll(failureStatus.getSubStatus());
      } else {
        failureStatusList.add(failureStatus);
      }
      if (failureStatusList.size() == 1) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    failureStatusList.get(0).getMessage(), failureStatusList.get(0).getCode())));
      } else {
        setFailure(
            new ProcedureException(
                new BatchProcessException(failureStatusList.toArray(new TSStatus[0]))));
      }
    }

    @Override
    protected void onAllReplicasetFailure(
        TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Alter view %s failed when [%s] because failed to execute in all replicaset of schemaRegion %s. Failure nodes: %s",
                      viewPathToSourceMap.keySet(),
                      taskName,
                      consensusGroupId.id,
                      dataNodeLocationSet))));
      interruptTask();
    }
  }
}
