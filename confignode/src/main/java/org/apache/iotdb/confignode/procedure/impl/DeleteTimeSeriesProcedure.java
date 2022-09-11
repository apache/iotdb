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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.ConstructSchemaBlackListHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteDataForDeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.DeleteTimeSeriesHandler;
import org.apache.iotdb.confignode.client.async.handlers.FetchSchemaBlackLsitHandler;
import org.apache.iotdb.confignode.client.async.handlers.InvalidateMatchedSchemaCacheHandler;
import org.apache.iotdb.confignode.client.async.handlers.RollbackSchemaBlackListHandler;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private PathPatternTree patternTree;
  private ByteBuffer patternTreeBytes;

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
      default:
        setFailure(new ProcedureException("Unrecognized state " + state.toString()));
        return Flow.NO_MORE_STATE;
    }
    return Flow.HAS_MORE_STATE;
  }

  private void constructBlackList(ConfigNodeProcedureEnv env) {
    RegionTask<TSStatus> constructBlackListTask =
        new RegionTask<TSStatus>(
            "construct schema black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree)) {
          @Override
          Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            // construct request and send
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            ConstructSchemaBlackListHandler handler =
                new ConstructSchemaBlackListHandler(dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(
                    new TConstructSchemaBlackListReq(consensusGroupIdList, patternTreeBytes),
                    handler);

            return handler.getDataNodeResponseStatusMap();
          }
        };
    constructBlackListTask.execute();

    if (!isFailed()) {
      setNextState(DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE);
    }
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    InvalidateMatchedSchemaCacheHandler handler =
        new InvalidateMatchedSchemaCacheHandler(dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance()
        .sendAsyncRequestToDataNodeWithRetry(
            new TInvalidateMatchedSchemaCacheReq(patternTreeBytes), handler);
    Map<Integer, TSStatus> statusMap = handler.getDataNodeResponseStatusMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schema cache of timeseries {}", requestMessage);
        setFailure(new ProcedureException("Invalidate schema cache failed"));
        return;
      }
    }

    setNextState(DeleteTimeSeriesState.DELETE_DATA);
  }

  private void deleteData(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeSchemaRegionGroupGroupIdMap =
        getLeaderDataNodeRegionGroupMap(
            env.getConfigManager().getPartitionManager().getAllLeadership(),
            relatedSchemaRegionGroup);

    // fetch schema black list by dataNode
    for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
        dataNodeSchemaRegionGroupGroupIdMap.entrySet()) {
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup = new HashMap<>();
      entry
          .getValue()
          .forEach(
              consensusGroupId ->
                  targetSchemaRegionGroup.put(
                      consensusGroupId, relatedSchemaRegionGroup.get(consensusGroupId)));
      PathPatternTree patternTree =
          fetchSchemaBlackListOnTargetDataNode(env, targetSchemaRegionGroup);
      if (isFailed()) {
        return;
      }
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
          env.getConfigManager().getRelatedDataRegionGroup(patternTree);

      // target timeseries has no data
      if (relatedDataRegionGroup.isEmpty()) {
        continue;
      }

      RegionTask<TSStatus> deleteDataTask =
          new RegionTask<TSStatus>("delete data", env, relatedDataRegionGroup) {
            @Override
            Map<Integer, TSStatus> sendRequest(
                TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
              Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
              dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
              DeleteDataForDeleteTimeSeriesHandler handler =
                  new DeleteDataForDeleteTimeSeriesHandler(dataNodeLocationMap);
              AsyncDataNodeClientPool.getInstance()
                  .sendAsyncRequestToDataNodeWithRetry(
                      new TDeleteDataForDeleteTimeSeriesReq(
                          new ArrayList<>(consensusGroupIdList),
                          preparePatternTreeBytesData(patternTree)),
                      handler);
              return handler.getDataNodeResponseStatusMap();
            }
          };
      deleteDataTask.setExecuteOnAllReplicaset(true);
      deleteDataTask.execute();
      if (isFailed()) {
        return;
      }
    }
    setNextState(DeleteTimeSeriesState.DELETE_TIMESERIES);
  }

  private PathPatternTree fetchSchemaBlackListOnTargetDataNode(
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
    RegionTask<TFetchSchemaBlackListResp> fetchSchemaBlackListTask =
        new RegionTask<TFetchSchemaBlackListResp>(
            "fetch schema black list", env, targetSchemaRegionGroup) {
          @Override
          Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            FetchSchemaBlackLsitHandler handler =
                new FetchSchemaBlackLsitHandler(dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(
                    new TFetchSchemaBlackListReq(consensusGroupIdList, patternTreeBytes), handler);
            Map<Integer, TFetchSchemaBlackListResp> respMap = handler.getDataNodeResponseMap();
            setResponseMap(respMap);
            Map<Integer, TSStatus> statusMap = new HashMap<>();
            respMap.forEach((k, v) -> statusMap.put(k, v.getStatus()));
            return statusMap;
          }
        };
    fetchSchemaBlackListTask.execute();
    if (isFailed()) {
      return null;
    }

    Map<Integer, TFetchSchemaBlackListResp> respMap = fetchSchemaBlackListTask.getResponseMap();
    PathPatternTree patternTree = new PathPatternTree();
    for (TFetchSchemaBlackListResp resp : respMap.values()) {
      for (PartialPath path :
          PathPatternTree.deserialize(ByteBuffer.wrap(resp.getPathPatternTree()))
              .getAllPathPatterns()) {
        patternTree.appendPathPattern(path);
      }
    }
    patternTree.constructTree();
    return patternTree;
  }

  private void deleteTimeSeries(ConfigNodeProcedureEnv env) {
    RegionTask<TSStatus> deleteTimeSeriesTask =
        new RegionTask<TSStatus>(
            "delete timeseries",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree)) {
          @Override
          Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            DeleteTimeSeriesHandler handler = new DeleteTimeSeriesHandler(dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(
                    new TDeleteTimeSeriesReq(consensusGroupIdList, patternTreeBytes), handler);
            return handler.getDataNodeResponseStatusMap();
          }
        };
    deleteTimeSeriesTask.execute();
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getLeaderDataNodeRegionGroupMap(
      Map<TConsensusGroupId, Integer> leaderMap,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap = new HashMap<>();
    regionReplicaSetMap.forEach(
        (consensusGroupId, regionReplicaSet) -> {
          Integer leaderId = leaderMap.get(consensusGroupId);
          TDataNodeLocation leaderDataNodeLocation = null;
          if (leaderId == null || leaderId == -1) {
            leaderDataNodeLocation = regionReplicaSet.getDataNodeLocations().get(0);
          } else {
            for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
              if (dataNodeLocation.getDataNodeId() == leaderId) {
                leaderDataNodeLocation = dataNodeLocation;
                break;
              }
            }
          }
          dataNodeConsensusGroupIdMap
              .computeIfAbsent(leaderDataNodeLocation, k -> new ArrayList<>())
              .add(regionReplicaSet.getRegionId());
        });
    return dataNodeConsensusGroupIdMap;
  }

  private Map<TDataNodeLocation, List<TConsensusGroupId>> getAllReplicaDataNodeRegionGroupMap(
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap = new HashMap<>();
    regionReplicaSetMap.forEach(
        (consensusGroupId, regionReplicaSet) -> {
          for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
            dataNodeConsensusGroupIdMap
                .computeIfAbsent(dataNodeLocation, k -> new ArrayList<>())
                .add(regionReplicaSet.getRegionId());
          }
        });
    return dataNodeConsensusGroupIdMap;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {
    RegionTask<TSStatus> rollbackStateTask =
        new RegionTask<TSStatus>(
            "roll back schema black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree)) {
          @Override
          Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            RollbackSchemaBlackListHandler handler =
                new RollbackSchemaBlackListHandler(dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(
                    new TRollbackSchemaBlackListReq(consensusGroupIdList, patternTreeBytes),
                    handler);
            return handler.getDataNodeResponseStatusMap();
          }
        };
    rollbackStateTask.execute();
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

  private abstract class RegionTask<T> {

    private final String taskName;
    private final ConfigNodeProcedureEnv env;
    private final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup;

    private boolean executeOnAllReplicaset = false;

    private Map<Integer, T> responseMap;

    RegionTask(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
      this.taskName = taskName;
      this.env = env;
      this.targetSchemaRegionGroup = targetSchemaRegionGroup;
    }

    private void execute() {
      // organize schema region by dataNode
      Set<TDataNodeLocation> allFailedDataNodeSet = new HashSet<>();
      Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap =
          executeOnAllReplicaset
              ? getAllReplicaDataNodeRegionGroupMap(targetSchemaRegionGroup)
              : getLeaderDataNodeRegionGroupMap(
                  env.getConfigManager().getPartitionManager().getAllLeadership(),
                  targetSchemaRegionGroup);
      while (!dataNodeConsensusGroupIdMap.isEmpty()) {
        Map<TDataNodeLocation, List<TConsensusGroupId>> currentFailedDataNodeMap =
            sendSchemaRegionRequest(dataNodeConsensusGroupIdMap);
        // some dataNode execution failure
        if (isFailed()) {
          return;
        }

        if (currentFailedDataNodeMap.isEmpty()) {
          break;
        }

        // retry failed dataNode requests caused by unexpected error on other replicates on other
        // dataNodes
        currentFailedDataNodeMap.forEach(dataNodeConsensusGroupIdMap::remove);
        // remove dataNodes that successfully executed request
        allFailedDataNodeSet.removeAll(dataNodeConsensusGroupIdMap.keySet());
        dataNodeConsensusGroupIdMap =
            getAvailableDataNodeLocationForRetry(currentFailedDataNodeMap, allFailedDataNodeSet);
        // some consensus group has no available dataNode
        if (isFailed()) {
          return;
        }
      }
    }

    private Map<TDataNodeLocation, List<TConsensusGroupId>> sendSchemaRegionRequest(
        Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeConsensusGroupIdMap) {
      // send request to each dataNode
      Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeMap = new HashMap<>();
      for (Map.Entry<TDataNodeLocation, List<TConsensusGroupId>> entry :
          dataNodeConsensusGroupIdMap.entrySet()) {
        // process response
        Map<Integer, TSStatus> dataNodeResponseMap = sendRequest(entry.getKey(), entry.getValue());
        TSStatus currentDataNodeResponse = dataNodeResponseMap.get(entry.getKey().getDataNodeId());
        if (currentDataNodeResponse.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          if (currentDataNodeResponse.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
            // dataNode execution error
            LOGGER.error(
                "Failed to execute [{}] of delete timeseries {} on {}",
                taskName,
                requestMessage,
                entry.getKey());
            setFailure(
                new ProcedureException(
                    String.format(
                        "Delete timeseries %s failed on when [%s]", requestMessage, taskName)));
            break;
          } else {
            // unexpected error, retry on other replicates on other dataNodes
            failedDataNodeMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
      return failedDataNodeMap;
    }

    private Map<TDataNodeLocation, List<TConsensusGroupId>> getAvailableDataNodeLocationForRetry(
        Map<TDataNodeLocation, List<TConsensusGroupId>> failedDataNodeConsensusGroupIdMap,
        Set<TDataNodeLocation> allFailedDataNodeSet) {
      Map<TConsensusGroupId, Integer> leaderMap =
          env.getConfigManager().getPartitionManager().getAllLeadership();
      Map<TDataNodeLocation, List<TConsensusGroupId>> availableDataNodeLocation = new HashMap<>();
      for (List<TConsensusGroupId> consensusGroupIdList :
          failedDataNodeConsensusGroupIdMap.values()) {
        for (TConsensusGroupId consensusGroupId : consensusGroupIdList) {
          TRegionReplicaSet regionReplicaSet = targetSchemaRegionGroup.get(consensusGroupId);
          TDataNodeLocation selectedDataNode = null;
          Integer leaderId = leaderMap.get(consensusGroupId);
          if (leaderId == null || leaderId == -1) {
            for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
              if (!allFailedDataNodeSet.contains(candidateDataNode)) {
                if (selectedDataNode == null) {
                  selectedDataNode = candidateDataNode;
                }
              }
            }
          } else {
            for (TDataNodeLocation candidateDataNode : regionReplicaSet.getDataNodeLocations()) {
              if (!allFailedDataNodeSet.contains(candidateDataNode)) {
                if (leaderId == candidateDataNode.getDataNodeId()) {
                  // retry on the new leader as possible
                  selectedDataNode = candidateDataNode;
                  break;
                }
                if (selectedDataNode == null) {
                  selectedDataNode = candidateDataNode;
                }
              }
            }
          }

          if (selectedDataNode == null) {
            setFailure(
                new ProcedureException(
                    String.format(
                        "Delete timeseries %s failed when [%s] because all replicaset of schemaRegion %s failed.",
                        requestMessage, taskName, consensusGroupId.id)));
            return availableDataNodeLocation;
          } else {
            availableDataNodeLocation
                .compute(selectedDataNode, (k, v) -> new ArrayList<>())
                .add(consensusGroupId);
          }
        }
      }
      return availableDataNodeLocation;
    }

    Map<Integer, T> getResponseMap() {
      return responseMap;
    }

    protected void setResponseMap(Map<Integer, T> responseMap) {
      this.responseMap = responseMap;
    }

    void setExecuteOnAllReplicaset(boolean executeOnAllReplicaset) {
      this.executeOnAllReplicaset = executeOnAllReplicaset;
    }

    abstract Map<Integer, TSStatus> sendRequest(
        TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList);
  }
}
