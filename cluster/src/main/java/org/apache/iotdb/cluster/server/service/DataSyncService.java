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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSyncService extends BaseSyncService implements TSDataService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(DataSyncService.class);
  private DataGroupMember dataGroupMember;

  public DataSyncService(DataGroupMember member) {
    super(member);
    this.dataGroupMember = member;
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request) throws TException {
    try {
      dataGroupMember.sendSnapshot(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public PullSnapshotResp pullSnapshot(PullSnapshotRequest request) throws TException {
    PullSnapshotResp pullSnapshotResp;
    try {
      pullSnapshotResp = dataGroupMember.pullSnapshot(request);
    } catch (IOException e) {
      throw new TException(e);
    }
    if (pullSnapshotResp == null) {
      return forwardPullSnapshot(request);
    } else {
      return pullSnapshotResp;
    }
  }

  private PullSnapshotResp forwardPullSnapshot(PullSnapshotRequest request) throws TException {
    // if this node has been set readOnly, then it must have been synchronized with the leader
    // otherwise forward the request to the leader
    if (dataGroupMember.getLeader() != null) {
      logger.debug("{} forwarding a pull snapshot request to the leader {}", name,
          dataGroupMember.getLeader());
      SyncDataClient client =
          (SyncDataClient) dataGroupMember.getSyncClient(dataGroupMember.getLeader());
      PullSnapshotResp pullSnapshotResp = client.pullSnapshot(request);
      putBackSyncClient(client);
      return pullSnapshotResp;
    } else {
      throw new TException(new LeaderUnknownException(dataGroupMember.getAllNodes()));
    }
  }

  @Override
  public PullSchemaResp pullTimeSeriesSchema(PullSchemaRequest request) throws TException {
    try {
      return dataGroupMember.pullTimeSeriesSchema(request);
    } catch (CheckConsistencyException e) {
      // if this node cannot synchronize with the leader with in a given time, forward the
      // request to the leader
      dataGroupMember.waitLeader();
      SyncDataClient client =
          (SyncDataClient) dataGroupMember.getSyncClient(dataGroupMember.getLeader());
      if (client == null) {
        throw new TException(new LeaderUnknownException(dataGroupMember.getAllNodes()));
      }
      PullSchemaResp pullSchemaResp = client.pullTimeSeriesSchema(request);
      putBackSyncClient(client);
      return pullSchemaResp;
    }
  }

  @Override
  public long querySingleSeries(SingleSeriesQueryRequest request) throws TException {
    try {
      return dataGroupMember.querySingleSeries(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public long querySingleSeriesByTimestamp(SingleSeriesQueryRequest request) throws TException {
    try {
      return dataGroupMember.querySingleSeriesByTimestamp(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void endQuery(Node header, Node requester, long queryId) throws TException {
    try {
      dataGroupMember.endQuery(requester, queryId);
    } catch (StorageEngineException e) {
      throw new TException(e);
    }
  }

  @Override
  public ByteBuffer fetchSingleSeries(Node header, long readerId) throws TException {
    try {
      return dataGroupMember.fetchSingleSeries(readerId);
    } catch (ReaderNotFoundException | IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public ByteBuffer fetchSingleSeriesByTimestamp(Node header, long readerId, long timestamp)
      throws TException {
    try {
      return dataGroupMember.fetchSingleSeriesByTimestamp(readerId, timestamp);
    } catch (ReaderNotFoundException | IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public List<String> getAllPaths(Node header, List<String> paths) throws TException {
    try {
      return dataGroupMember.getAllPaths(paths);
    } catch (MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public Set<String> getAllDevices(Node header, List<String> path) throws TException {
    try {
      return dataGroupMember.getAllDevices(path);
    } catch (MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public List<String> getNodeList(Node header, String path, int nodeLevel) throws TException {
    try {
      return dataGroupMember.getNodeList(path, nodeLevel);
    } catch (CheckConsistencyException | MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(Node header, String path) throws TException {
    try {
      return dataGroupMember.getChildNodePathInNextLevel(path);
    } catch (CheckConsistencyException | MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public ByteBuffer getAllMeasurementSchema(Node header, ByteBuffer planBinary) throws TException {
    try {
      return dataGroupMember.getAllMeasurementSchema(planBinary);
    } catch (CheckConsistencyException | IOException | MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public List<ByteBuffer> getAggrResult(GetAggrResultRequest request) throws TException {
    try {
      return dataGroupMember.getAggrResult(request);
    } catch (StorageEngineException | QueryProcessException | IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public List<String> getUnregisteredTimeseries(Node header, List<String> timeseriesList)
      throws TException {
    try {
      return dataGroupMember.getUnregisteredTimeseries(timeseriesList);
    } catch (CheckConsistencyException e) {
      throw new TException(e);
    }
  }

  @Override
  public long getGroupByExecutor(GroupByRequest request) throws TException {
    try {
      return dataGroupMember.getGroupByExecutor(request);
    } catch (QueryProcessException | StorageEngineException e) {
      throw new TException(e);
    }
  }

  @Override
  public List<ByteBuffer> getGroupByResult(Node header, long executorId, long startTime, long endTime)
      throws TException {
    try {
      return dataGroupMember.getGroupByResult(executorId, startTime, endTime);
    } catch (ReaderNotFoundException | IOException | QueryProcessException e) {
      throw new TException(e);
    }
  }

  @Override
  public ByteBuffer previousFill(PreviousFillRequest request) throws TException {
    try {
      return dataGroupMember.previousFill(request);
    } catch (QueryProcessException | StorageEngineException | IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public ByteBuffer last(LastQueryRequest request) throws TException {
    try {
      return dataGroupMember.last(request);
    } catch (CheckConsistencyException | QueryProcessException | IOException | StorageEngineException e) {
      throw new TException(e);
    }
  }

  @Override
  public int getPathCount(Node header, List<String> pathsToQuery, int level) throws TException {
    try {
      return dataGroupMember.getPathCount(pathsToQuery, level);
    } catch (CheckConsistencyException | MetadataException e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean onSnapshotApplied(Node header, List<Integer> slots) {
    return dataGroupMember.onSnapshotApplied(slots);
  }
}
