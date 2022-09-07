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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.ConstructSchemaBlackListHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteDataForDeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.FetchSchemaBlackLsitHandler;
import org.apache.iotdb.confignode.client.async.handlers.InvalidateMatchedSchemaCacheHandler;
import org.apache.iotdb.confignode.client.async.handlers.RollbackSchemaBlackListHandler;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private static final int RETRY_THRESHOLD = 5;

  private PathPatternTree patternTree;
  private byte[] patternTreeBytes;

  private String requestMessage;

  public DeleteTimeSeriesProcedure() {
    super();
  }

  public DeleteTimeSeriesProcedure(PathPatternTree patternTree) {
    super();
    setPatternTree(patternTree);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schema black list of timeseries {}", requestMessage);
          constructBlackList(env);
          break;
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of timeseries {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of timeseries {}", requestMessage);
          deleteData(env);
          break;
        case DELETE_TIMESERIES:
          LOGGER.info("Delete timeseries schema of {}", requestMessage);
          deleteTimeSeries(env);
          return Flow.NO_MORE_STATE;
      }
    } catch (TException | IOException e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException("Delete timeseries failed " + state));
      } else {
        LOGGER.error(
            "Retry error trying to delete timeseries {}, state {}", requestMessage, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(new ProcedureException("State stuck at " + state));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private void constructBlackList(ConfigNodeProcedureEnv env) throws TException, IOException {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        getDataNodeRegionGroupMap(relatedSchemaRegionGroup);

    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      List<TSStatus> statusList = new ArrayList<>();
      Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
      dataNodeLocationMap.put(entry.getKey().getDataNodeId(), entry.getKey());
      ConstructSchemaBlackListHandler handler =
          new ConstructSchemaBlackListHandler(dataNodeLocationMap, statusList);
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TConstructSchemaBlackListReq(entry.getValue(), ByteBuffer.wrap(patternTreeBytes)),
              handler);
      if (statusList.get(statusList.size() - 1).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // todo implement retry on another dataNode
        LOGGER.error(
            "Failed to construct schema black list of timeseries {} on {}",
            requestMessage,
            entry.getKey());
        setFailure(new ProcedureException("Construct schema black list failed"));
        return;
      }
    }
    setNextState(DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE);
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) throws TException, IOException {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    List<TSStatus> statusList = new ArrayList<>();
    InvalidateMatchedSchemaCacheHandler handler =
        new InvalidateMatchedSchemaCacheHandler(dataNodeLocationMap, statusList);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            new TInvalidateMatchedSchemaCacheReq(ByteBuffer.wrap(patternTreeBytes)), handler);
    for (TSStatus status : statusList) {
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // todo implement retry on another dataNode
        LOGGER.error("Failed to invalidate schema cache of timeseries {}", requestMessage);
        setFailure(new ProcedureException("Invalidate schema cache failed"));
        return;
      }
    }
    setNextState(DeleteTimeSeriesState.DELETE_DATA);
  }

  private void deleteData(ConfigNodeProcedureEnv env) throws TException, IOException {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeSchemaRegionGroupGroupIdMap =
        getDataNodeRegionGroupMap(relatedSchemaRegionGroup);

    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeSchemaRegionGroupGroupIdMap.entrySet()) {
      PathPatternTree patternTree =
          fetchSchemaBlackListOnTargetDataNode(entry.getKey(), entry.getValue());
      if (patternTree == null) {
        LOGGER.error(
            "Failed to fetch schema black list for delete data of timeseries {} on {}",
            requestMessage,
            entry.getKey());
        setFailure(new ProcedureException("Fetch schema black list forDelete data failed"));
        return;
      }

      if (patternTree.isEmpty()) {
        continue;
      }

      Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
          getRelatedDataRegionGroup(env, patternTree);
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeDataRegionGroupIdMap =
          getDataNodeRegionGroupMap(relatedDataRegionGroup);

      List<TSStatus> statusList = new ArrayList<>();
      Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
      dataNodeDataRegionGroupIdMap.forEach((k, v) -> dataNodeLocationMap.put(k.getDataNodeId(), k));
      DeleteDataForDeleteTimeSeriesHandler handler =
          new DeleteDataForDeleteTimeSeriesHandler(dataNodeLocationMap, statusList);
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TDeleteDataForDeleteTimeSeriesReq(
                  entry.getValue(), ByteBuffer.wrap(patternTreeBytes)),
              handler);
      if (statusList.get(statusList.size() - 1).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // todo implement retry on another dataNode
        LOGGER.error(
            "Failed to delete data of timeseries {} on {}",
            patternTree.getAllPathPatterns().toString(),
            entry.getKey());
        setFailure(new ProcedureException("Delete data failed"));
        return;
      }
    }
    setNextState(DeleteTimeSeriesState.DELETE_TIMESERIES);
  }

  private PathPatternTree fetchSchemaBlackListOnTargetDataNode(
      TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> schemaRegionIdList) {
    List<TFetchSchemaBlackListResp> respList = new ArrayList<>();
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
    dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
    FetchSchemaBlackLsitHandler handler =
        new FetchSchemaBlackLsitHandler(dataNodeLocationMap, respList);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            new TFetchSchemaBlackListReq(schemaRegionIdList, ByteBuffer.wrap(patternTreeBytes)),
            handler);
    if (respList.get(respList.size() - 1).getStatus().getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // todo implement retry on another dataNode
      LOGGER.error(
          "Failed to fetch schema black list for delete data of timeseries {} on {}",
          requestMessage,
          dataNodeLocation);
      setFailure(new ProcedureException("Fetch schema black list forDelete data failed"));
      return null;
    }

    return PathPatternTree.deserialize(
        ByteBuffer.wrap(respList.get(respList.size() - 1).getPathPatternTree()));
  }

  private void deleteTimeSeries(ConfigNodeProcedureEnv env) throws TException, IOException {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        getDataNodeRegionGroupMap(relatedSchemaRegionGroup);

    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      List<TSStatus> statusList = new ArrayList<>();
      Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
      dataNodeLocationMap.put(entry.getKey().getDataNodeId(), entry.getKey());
      DeleteTimeSeriesHandler handler =
          new DeleteTimeSeriesHandler(dataNodeLocationMap, statusList);
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TDeleteTimeSeriesReq(entry.getValue(), ByteBuffer.wrap(patternTreeBytes)),
              handler);
      if (statusList.get(statusList.size() - 1).getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // todo implement retry on another dataNode
        LOGGER.error("Failed to delete timeseries {} on {}", requestMessage, entry.getKey());
        setFailure(new ProcedureException("Delete timeseries failed"));
        return;
      }
    }
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
      ConfigNodeProcedureEnv env) {
    ConfigManager configManager = env.getConfigManager();
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        configManager.getSchemaPartition(patternTree).getSchemaPartitionTable();
    List<TRegionReplicaSet> allRegionReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        schemaPartitionTable.values().stream()
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toSet());
    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getRelatedDataRegionGroup(
      ConfigNodeProcedureEnv env, PathPatternTree patternTree) {
    ConfigManager configManager = env.getConfigManager();
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        configManager.getSchemaPartition(patternTree).getSchemaPartitionTable();
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    schemaPartitionTable.forEach(
        (key, value) -> {
          Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> slotListMap = new HashMap<>();
          value.keySet().forEach(slot -> slotListMap.put(slot, Collections.emptyList()));
          partitionSlotsMap.put(key, slotListMap);
        });
    GetDataPartitionPlan getDataPartitionPlan = new GetDataPartitionPlan(partitionSlotsMap);
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        dataPartitionTable =
            configManager.getDataPartition(getDataPartitionPlan).getDataPartitionTable();

    List<TRegionReplicaSet> allRegionReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        dataPartitionTable.values().stream()
            .flatMap(
                tSeriesPartitionSlotMapMap ->
                    tSeriesPartitionSlotMapMap.values().stream()
                        .flatMap(
                            tTimePartitionSlotListMap ->
                                tTimePartitionSlotListMap.values().stream()
                                    .flatMap(Collection::stream)))
            .collect(Collectors.toSet());

    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getDataNodeRegionGroupMap(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap = new HashMap<>();
    regionReplicaSetMap
        .values()
        .forEach(
            regionReplicaSet -> {
              dataNodeConsensusGroupIdMap
                  .computeIfAbsent(
                      regionReplicaSet.getDataNodeLocations().get(0), k -> new ArrayList<>())
                  .add(regionReplicaSet.getRegionId());
            });
    return dataNodeConsensusGroupIdMap;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {
    rollbackBlackList(configNodeProcedureEnv);
  }

  private void rollbackBlackList(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
        getDataNodeRegionGroupMap(relatedSchemaRegionGroup);

    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeConsensusGroupIdMap.entrySet()) {
      List<TSStatus> statusList = new ArrayList<>();
      Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
      dataNodeLocationMap.put(entry.getKey().getDataNodeId(), entry.getKey());
      RollbackSchemaBlackListHandler handler =
          new RollbackSchemaBlackListHandler(dataNodeLocationMap, statusList);
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TRollbackSchemaBlackListReq(entry.getValue(), ByteBuffer.wrap(patternTreeBytes)),
              handler);
      if (statusList.get(0).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // todo implement retry on another dataNode
        LOGGER.error(
            "Failed to rollback schema black list of timeseries {} on {}",
            requestMessage,
            entry.getKey());
        setFailure(new ProcedureException("Construct schema black list failed"));
        return;
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(DeleteTimeSeriesState deleteTimeSeriesState) {
    return true;
  }

  @Override
  protected DeleteTimeSeriesState getState(int stateId) {
    return DeleteTimeSeriesState.values()[stateId];
  }

  @Override
  protected int getStateId(DeleteTimeSeriesState deleteTimeSeriesState) {
    return deleteTimeSeriesState.ordinal();
  }

  @Override
  protected DeleteTimeSeriesState getInitialState() {
    return DeleteTimeSeriesState.CONSTRUCT_BLACK_LIST;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public void setPatternTree(PathPatternTree patternTree) {
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    preparePatternTreeBytesData();
  }

  private void preparePatternTreeBytesData() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {

    }
    patternTreeBytes = byteArrayOutputStream.toByteArray();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DELETE_TIMESERIES_PROCEDURE.ordinal());
    super.serialize(stream);
    patternTree.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeleteTimeSeriesProcedure that = (DeleteTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getState() == that.getState()
        && patternTree.equals(that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), patternTree);
  }
}
