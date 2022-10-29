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
import org.apache.iotdb.confignode.procedure.state.schema.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionGroupUtil.getLeaderDataNodeRegionGroupMap;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private String queryId;

  private PathPatternTree patternTree;
  private ByteBuffer patternTreeBytes;

  private String requestMessage;

  public DeleteTimeSeriesProcedure() {
    super();
  }

  public DeleteTimeSeriesProcedure(String queryId, PathPatternTree patternTree) {
    super();
    this.queryId = queryId;
    setPatternTree(patternTree);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeleteTimeSeriesState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schema black list of timeseries {}", requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            setFailure(
                new ProcedureException(
                    new PathNotExistException(
                        patternTree.getAllPathPatterns().stream()
                            .map(PartialPath::getFullPath)
                            .collect(Collectors.toList()))));
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of timeseries {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of timeseries {}", requestMessage);
          deleteData(env);
          break;
        case DELETE_TIMESERIES_SCHEMA:
          LOGGER.info("Delete timeseries schema of {}", requestMessage);
          deleteTimeSeriesSchema(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "DeleteTimeSeries-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  // return the total num of timeseries in schema black list
  private int constructBlackList(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    DeleteTimeSeriesRegionTask<TSStatus> constructBlackListTask =
        new DeleteTimeSeriesRegionTask<TSStatus>(
            "construct schema black list", env, targetSchemaRegionGroup) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            // construct request and send
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);

            AsyncClientHandler<TConstructSchemaBlackListReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.CONSTRUCT_SCHEMA_BLACK_LIST,
                    new TConstructSchemaBlackListReq(consensusGroupIdList, patternTreeBytes),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    constructBlackListTask.execute();

    if (isFailed()) {
      return 0;
    }

    int preDeletedNum = 0;
    for (List<TSStatus> respList : constructBlackListTask.getResponseMap().values()) {
      for (TSStatus resp : respList) {
        preDeletedNum += Integer.parseInt(resp.getMessage());
      }
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
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schema cache of timeseries {}", requestMessage);
        setFailure(new ProcedureException(new MetadataException("Invalidate schema cache failed")));
        return;
      }
    }

    setNextState(DeleteTimeSeriesState.DELETE_DATA);
  }

  private void deleteData(ConfigNodeProcedureEnv env) {
    deleteDataWithRawPathPattern(env);
  }

  private void deleteDataWithRawPathPattern(ConfigNodeProcedureEnv env) {
    executeDeleteData(env, patternTree);
    if (isFailed()) {
      return;
    }
    setNextState(DeleteTimeSeriesState.DELETE_TIMESERIES_SCHEMA);
  }

  // todo this will be used in IDTable scenarios
  private void deleteDataWithResolvedPath(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    Map<TDataNodeLocation, List<TConsensusGroupId>> dataNodeSchemaRegionGroupGroupIdMap =
        getLeaderDataNodeRegionGroupMap(
            env.getConfigManager().getLoadManager().getLatestRegionLeaderMap(),
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
      // resolve original path pattern into specific timeseries full path
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
        setFailure(
            new ProcedureException(
                new MetadataException("Fetch schema black list forDelete data failed")));
        return;
      }

      if (patternTree.isEmpty()) {
        continue;
      }

      executeDeleteData(env, patternTree);

      if (isFailed()) {
        return;
      }
    }
    setNextState(DeleteTimeSeriesState.DELETE_TIMESERIES_SCHEMA);
  }

  private void executeDeleteData(ConfigNodeProcedureEnv env, PathPatternTree patternTree) {
    Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(patternTree);

    // target timeseries has no data
    if (relatedDataRegionGroup.isEmpty()) {
      return;
    }

    DeleteTimeSeriesRegionTask<TSStatus> deleteDataTask =
        new DeleteTimeSeriesRegionTask<TSStatus>("delete data", env, relatedDataRegionGroup, true) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TDeleteDataForDeleteSchemaReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
                    new TDeleteDataForDeleteSchemaReq(
                        new ArrayList<>(consensusGroupIdList),
                        preparePatternTreeBytesData(patternTree)),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    deleteDataTask.execute();
  }

  private PathPatternTree fetchSchemaBlackListOnTargetDataNode(
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
    DeleteTimeSeriesRegionTask<TFetchSchemaBlackListResp> fetchSchemaBlackListTask =
        new DeleteTimeSeriesRegionTask<TFetchSchemaBlackListResp>(
            "fetch schema black list", env, targetSchemaRegionGroup) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TFetchSchemaBlackListReq, TFetchSchemaBlackListResp> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.FETCH_SCHEMA_BLACK_LIST,
                    new TFetchSchemaBlackListReq(consensusGroupIdList, patternTreeBytes),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            Map<Integer, TSStatus> statusMap = new HashMap<>();
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                      statusMap.put(k, v.getStatus());
                    });
            return statusMap;
          }
        };
    fetchSchemaBlackListTask.execute();
    if (isFailed()) {
      return null;
    }

    Map<Integer, List<TFetchSchemaBlackListResp>> respMap =
        fetchSchemaBlackListTask.getResponseMap();
    PathPatternTree patternTree = new PathPatternTree();
    for (List<TFetchSchemaBlackListResp> respList : respMap.values()) {
      for (TFetchSchemaBlackListResp resp : respList) {
        for (PartialPath path :
            PathPatternTree.deserialize(ByteBuffer.wrap(resp.getPathPatternTree()))
                .getAllPathPatterns()) {
          patternTree.appendFullPath(path);
        }
      }
    }
    patternTree.constructTree();
    return patternTree;
  }

  private void deleteTimeSeriesSchema(ConfigNodeProcedureEnv env) {
    DeleteTimeSeriesRegionTask<TSStatus> deleteTimeSeriesTask =
        new DeleteTimeSeriesRegionTask<TSStatus>(
            "delete timeseries schema",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree)) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TDeleteTimeSeriesReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.DELETE_TIMESERIES,
                    new TDeleteTimeSeriesReq(consensusGroupIdList, patternTreeBytes),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    deleteTimeSeriesTask.execute();
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {
    DeleteTimeSeriesRegionTask<TSStatus> rollbackStateTask =
        new DeleteTimeSeriesRegionTask<TSStatus>(
            "roll back schema black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree)) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TRollbackSchemaBlackListReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.ROLLBACK_SCHEMA_BLACK_LIST,
                    new TRollbackSchemaBlackListReq(consensusGroupIdList, patternTreeBytes),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
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
    stream.writeInt(ProcedureFactory.ProcedureType.DELETE_TIMESERIES_PROCEDURE.ordinal());
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
    DeleteTimeSeriesProcedure that = (DeleteTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getState() == that.getState()
        && patternTree.equals(that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), patternTree);
  }

  private abstract class DeleteTimeSeriesRegionTask<T> extends DataNodeRegionTask<T> {

    private final String taskName;

    DeleteTimeSeriesRegionTask(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
      super(env, targetSchemaRegionGroup, false);
      this.taskName = taskName;
    }

    DeleteTimeSeriesRegionTask(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        boolean executeOnAllReplicaset) {
      super(env, targetSchemaRegionGroup, executeOnAllReplicaset);
      this.taskName = taskName;
    }

    @Override
    protected boolean hasFailure() {
      return isFailed();
    }

    @Override
    protected void onExecutionFailure(TDataNodeLocation dataNodeLocation) {
      LOGGER.error(
          "Failed to execute [{}] of delete timeseries {} on {}",
          taskName,
          requestMessage,
          dataNodeLocation);
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Delete timeseries %s failed when [%s]", requestMessage, taskName))));
    }

    @Override
    protected void onAllReplicasetFailure(TConsensusGroupId consensusGroupId) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Delete timeseries %s failed when [%s] because all replicaset of schemaRegion %s failed.",
                      requestMessage, taskName, consensusGroupId.id))));
    }
  }
}
