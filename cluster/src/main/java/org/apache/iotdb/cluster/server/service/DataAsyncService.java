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

package org.apache.iotdb.cluster.server.service;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.MultSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.IoTDB;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataAsyncService extends BaseAsyncService implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataAsyncService.class);
  private DataGroupMember dataGroupMember;

  public DataAsyncService(DataGroupMember member) {
    super(member);
    this.dataGroupMember = member;
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback<Void> resultHandler) {
    try {
      dataGroupMember.receiveSnapshot(request);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void pullSnapshot(
      PullSnapshotRequest request, AsyncMethodCallback<PullSnapshotResp> resultHandler) {
    PullSnapshotResp pullSnapshotResp = null;
    try {
      pullSnapshotResp = dataGroupMember.getSnapshot(request);
    } catch (IOException e) {
      resultHandler.onError(e);
    }
    if (pullSnapshotResp == null) {
      forwardPullSnapshot(request, resultHandler);
    } else {
      resultHandler.onComplete(pullSnapshotResp);
    }
  }

  private void forwardPullSnapshot(
      PullSnapshotRequest request, AsyncMethodCallback<PullSnapshotResp> resultHandler) {
    // if this node has been set readOnly, then it must have been synchronized with the leader
    // otherwise forward the request to the leader
    if (dataGroupMember.getLeader() != null
        && !ClusterConstant.EMPTY_NODE.equals(dataGroupMember.getLeader())) {
      logger.debug(
          "{} forwarding a pull snapshot request to the leader {}",
          name,
          dataGroupMember.getLeader());
      AsyncDataClient client =
          (AsyncDataClient) dataGroupMember.getAsyncClient(dataGroupMember.getLeader());
      try {
        client.pullSnapshot(request, resultHandler);
      } catch (TException e) {
        resultHandler.onError(e);
      }
    } else {
      resultHandler.onError(new LeaderUnknownException(dataGroupMember.getAllNodes()));
    }
  }

  /**
   * forward the request to the leader
   *
   * @param request pull schema request
   * @param resultHandler result handler
   */
  @Override
  public void pullTimeSeriesSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    if (dataGroupMember.getCharacter() == NodeCharacter.LEADER) {
      try {
        resultHandler.onComplete(
            dataGroupMember.getLocalQueryExecutor().queryTimeSeriesSchema(request));
        return;
      } catch (CheckConsistencyException | MetadataException e) {
        // maybe the partition table of this node is not up-to-date, try again after updating
        // partition table
        try {
          dataGroupMember.getMetaGroupMember().syncLeaderWithConsistencyCheck(false);
          resultHandler.onComplete(
              dataGroupMember.getLocalQueryExecutor().queryTimeSeriesSchema(request));
          return;
        } catch (CheckConsistencyException | MetadataException ex) {
          resultHandler.onError(ex);
        }
      }
    }

    // forward the request to the leader
    AsyncDataClient leaderClient = getLeaderClient();
    if (leaderClient == null) {
      resultHandler.onError(new LeaderUnknownException(dataGroupMember.getAllNodes()));
      return;
    }
    try {
      leaderClient.pullTimeSeriesSchema(request, resultHandler);
    } catch (TException e1) {
      resultHandler.onError(e1);
    }
  }

  private AsyncDataClient getLeaderClient() {
    dataGroupMember.waitLeader();
    return (AsyncDataClient) dataGroupMember.getAsyncClient(dataGroupMember.getLeader());
  }

  /**
   * forward the request to the leader
   *
   * @param request pull schema request
   * @param resultHandler result handler
   */
  @Override
  public void pullMeasurementSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    if (dataGroupMember.getCharacter() == NodeCharacter.LEADER) {
      try {
        resultHandler.onComplete(
            dataGroupMember.getLocalQueryExecutor().queryMeasurementSchema(request));
        return;
      } catch (CheckConsistencyException | MetadataException e) {
        // maybe the partition table of this node is not up-to-date, try again after updating
        // partition table
        try {
          dataGroupMember.getMetaGroupMember().syncLeaderWithConsistencyCheck(false);
          resultHandler.onComplete(
              dataGroupMember.getLocalQueryExecutor().queryMeasurementSchema(request));
          return;
        } catch (CheckConsistencyException | MetadataException ex) {
          resultHandler.onError(ex);
        }
      }
    }

    // forward the request to the leader
    AsyncDataClient leaderClient = getLeaderClient();
    if (leaderClient == null) {
      resultHandler.onError(new LeaderUnknownException(dataGroupMember.getAllNodes()));
      return;
    }
    try {
      leaderClient.pullMeasurementSchema(request, resultHandler);
    } catch (TException e1) {
      resultHandler.onError(e1);
    }
  }

  @Override
  public void querySingleSeries(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().querySingleSeries(request));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void queryMultSeries(
      MultSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) throws TException {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().queryMultSeries(request));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void querySingleSeriesByTimestamp(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().querySingleSeriesByTimestamp(request));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void endQuery(
      RaftNode header, Node requester, long queryId, AsyncMethodCallback<Void> resultHandler) {
    try {
      dataGroupMember.getQueryManager().endQuery(requester, queryId);
      resultHandler.onComplete(null);
    } catch (StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void fetchSingleSeries(
      RaftNode header, long readerId, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().fetchSingleSeries(readerId));
    } catch (ReaderNotFoundException | IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void fetchMultSeries(
      RaftNode header,
      long readerId,
      List<String> paths,
      AsyncMethodCallback<Map<String, ByteBuffer>> resultHandler)
      throws TException {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().fetchMultSeries(readerId, paths));
    } catch (ReaderNotFoundException | IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void fetchSingleSeriesByTimestamps(
      RaftNode header,
      long readerId,
      List<Long> timestamps,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember
              .getLocalQueryExecutor()
              .fetchSingleSeriesByTimestamps(
                  readerId, timestamps.stream().mapToLong(k -> k).toArray(), timestamps.size()));
    } catch (ReaderNotFoundException | IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAllPaths(
      RaftNode header,
      List<String> paths,
      boolean withAlias,
      AsyncMethodCallback<GetAllPathsResult> resultHandler) {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
      resultHandler.onComplete(((CMManager) IoTDB.metaManager).getAllPaths(paths, withAlias));
    } catch (MetadataException | CheckConsistencyException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAllDevices(
      RaftNode header, List<String> path, AsyncMethodCallback<Set<String>> resultHandler) {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
      resultHandler.onComplete(((CMManager) IoTDB.metaManager).getAllDevices(path));
    } catch (MetadataException | CheckConsistencyException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getDevices(
      RaftNode header, ByteBuffer planBinary, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().getDevices(planBinary));
    } catch (CheckConsistencyException | IOException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getNodeList(
      RaftNode header,
      String path,
      int nodeLevel,
      AsyncMethodCallback<List<String>> resultHandler) {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
      resultHandler.onComplete(((CMManager) IoTDB.metaManager).getNodeList(path, nodeLevel));
    } catch (CheckConsistencyException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getChildNodeInNextLevel(
      RaftNode header, String path, AsyncMethodCallback<Set<String>> resultHandler) {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
      resultHandler.onComplete(((CMManager) IoTDB.metaManager).getChildNodeInNextLevel(path));
    } catch (CheckConsistencyException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getChildNodePathInNextLevel(
      RaftNode header, String path, AsyncMethodCallback<Set<String>> resultHandler) {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
      resultHandler.onComplete(((CMManager) IoTDB.metaManager).getChildNodePathInNextLevel(path));
    } catch (CheckConsistencyException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAllMeasurementSchema(
      RaftNode header, ByteBuffer planBinary, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().getAllMeasurementSchema(planBinary));
    } catch (CheckConsistencyException | IOException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getAggrResult(
      GetAggrResultRequest request, AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().getAggrResult(request));
    } catch (StorageEngineException | QueryProcessException | IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getUnregisteredTimeseries(
      RaftNode header,
      List<String> timeseriesList,
      AsyncMethodCallback<List<String>> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().getUnregisteredTimeseries(timeseriesList));
    } catch (CheckConsistencyException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getGroupByExecutor(GroupByRequest request, AsyncMethodCallback<Long> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().getGroupByExecutor(request));
    } catch (QueryProcessException | StorageEngineException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getGroupByResult(
      RaftNode header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().getGroupByResult(executorId, startTime, endTime));
    } catch (ReaderNotFoundException | IOException | QueryProcessException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void previousFill(
      PreviousFillRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().previousFill(request));
    } catch (QueryProcessException
        | StorageEngineException
        | IOException
        | IllegalPathException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void last(LastQueryRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(dataGroupMember.getLocalQueryExecutor().last(request));
    } catch (CheckConsistencyException
        | QueryProcessException
        | IOException
        | StorageEngineException
        | IllegalPathException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getPathCount(
      RaftNode header,
      List<String> pathsToQuery,
      int level,
      AsyncMethodCallback<Integer> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().getPathCount(pathsToQuery, level));
    } catch (CheckConsistencyException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void getDeviceCount(
      RaftNode header, List<String> pathsToQuery, AsyncMethodCallback<Integer> resultHandler)
      throws TException {
    try {
      resultHandler.onComplete(
          dataGroupMember.getLocalQueryExecutor().getDeviceCount(pathsToQuery));
    } catch (CheckConsistencyException | MetadataException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void onSnapshotApplied(
      RaftNode header, List<Integer> slots, AsyncMethodCallback<Boolean> resultHandler) {
    resultHandler.onComplete(dataGroupMember.onSnapshotInstalled(slots));
  }

  @Override
  public void peekNextNotNullValue(
      RaftNode header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(
          dataGroupMember
              .getLocalQueryExecutor()
              .peekNextNotNullValue(executorId, startTime, endTime));
    } catch (ReaderNotFoundException | IOException e) {
      resultHandler.onError(e);
    }
  }
}
