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

import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.MultSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataGroupServiceImpls implements TSDataService.AsyncIface, TSDataService.Iface {

  @Override
  public void sendHeartbeat(
      HeartBeatRequest request, AsyncMethodCallback<HeartBeatResponse> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.sendHeartbeat(request, resultHandler);
    }
  }

  @Override
  public void startElection(ElectionRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.startElection(request, resultHandler);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.appendEntries(request, resultHandler);
    }
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.appendEntry(request, resultHandler);
    }
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback<Void> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.sendSnapshot(request, resultHandler);
    }
  }

  @Override
  public void pullSnapshot(
      PullSnapshotRequest request, AsyncMethodCallback<PullSnapshotResp> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.pullSnapshot(request, resultHandler);
    }
  }

  @Override
  public void executeNonQueryPlan(
      ExecutNonQueryReq request, AsyncMethodCallback<TSStatus> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.executeNonQueryPlan(request, resultHandler);
    }
  }

  @Override
  public void requestCommitIndex(
      RaftNode header, AsyncMethodCallback<RequestCommitIndexResponse> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Request commit index");
    if (service != null) {
      service.requestCommitIndex(header, resultHandler);
    }
  }

  @Override
  public void readFile(
      String filePath, long offset, int length, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(IOUtils.readFile(filePath, offset, length));
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void querySingleSeries(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(
                request.getHeader(), resultHandler, "Query series:" + request.getPath());
    if (service != null) {
      service.querySingleSeries(request, resultHandler);
    }
  }

  @Override
  public void queryMultSeries(
      MultSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) throws TException {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(
                request.getHeader(), resultHandler, "Query series:" + request.getPath());
    if (service != null) {
      service.queryMultSeries(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeries(
      RaftNode header, long readerId, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Fetch reader:" + readerId);
    if (service != null) {
      service.fetchSingleSeries(header, readerId, resultHandler);
    }
  }

  @Override
  public void fetchMultSeries(
      RaftNode header,
      long readerId,
      List<String> paths,
      AsyncMethodCallback<Map<String, ByteBuffer>> resultHandler)
      throws TException {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Fetch reader:" + readerId);
    if (service != null) {
      service.fetchMultSeries(header, readerId, paths, resultHandler);
    }
  }

  @Override
  public void getAllPaths(
      RaftNode header,
      List<String> paths,
      boolean withAlias,
      AsyncMethodCallback<GetAllPathsResult> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Find path:" + paths);
    if (service != null) {
      service.getAllPaths(header, paths, withAlias, resultHandler);
    }
  }

  @Override
  public void endQuery(
      RaftNode header, Node thisNode, long queryId, AsyncMethodCallback<Void> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "End query");
    if (service != null) {
      service.endQuery(header, thisNode, queryId, resultHandler);
    }
  }

  @Override
  public void querySingleSeriesByTimestamp(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(
                request.getHeader(),
                resultHandler,
                "Query by timestamp:"
                    + request.getQueryId()
                    + "#"
                    + request.getPath()
                    + " of "
                    + request.getRequester());
    if (service != null) {
      service.querySingleSeriesByTimestamp(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeriesByTimestamps(
      RaftNode header,
      long readerId,
      List<Long> timestamps,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Fetch by timestamp:" + readerId);
    if (service != null) {
      service.fetchSingleSeriesByTimestamps(header, readerId, timestamps, resultHandler);
    }
  }

  @Override
  public void pullTimeSeriesSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.pullTimeSeriesSchema(request, resultHandler);
    }
  }

  @Override
  public void pullMeasurementSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, "Pull measurement schema");
    if (service != null) {
      service.pullMeasurementSchema(request, resultHandler);
    }
  }

  @Override
  public void getAllDevices(
      RaftNode header, List<String> paths, AsyncMethodCallback<Set<String>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "Get all devices");
    if (service != null) {
      service.getAllDevices(header, paths, resultHandler);
    }
  }

  @Override
  public void getDevices(
      RaftNode header, ByteBuffer planBinary, AsyncMethodCallback<ByteBuffer> resultHandler)
      throws TException {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "get devices");
    if (service != null) {
      service.getDevices(header, planBinary, resultHandler);
    }
  }

  @Override
  public void getNodeList(
      RaftNode header,
      String path,
      int nodeLevel,
      AsyncMethodCallback<List<String>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "Get node list");
    if (service != null) {
      service.getNodeList(header, path, nodeLevel, resultHandler);
    }
  }

  @Override
  public void getChildNodeInNextLevel(
      RaftNode header, String path, AsyncMethodCallback<Set<String>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Get child node in next level");
    if (service != null) {
      service.getChildNodeInNextLevel(header, path, resultHandler);
    }
  }

  @Override
  public void getChildNodePathInNextLevel(
      RaftNode header, String path, AsyncMethodCallback<Set<String>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Get child node path in next level");
    if (service != null) {
      service.getChildNodePathInNextLevel(header, path, resultHandler);
    }
  }

  @Override
  public void getAllMeasurementSchema(
      RaftNode header, ByteBuffer planBytes, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Get all measurement schema");
    if (service != null) {
      service.getAllMeasurementSchema(header, planBytes, resultHandler);
    }
  }

  @Override
  public void getAggrResult(
      GetAggrResultRequest request, AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.getAggrResult(request, resultHandler);
    }
  }

  @Override
  public void getUnregisteredTimeseries(
      RaftNode header,
      List<String> timeseriesList,
      AsyncMethodCallback<List<String>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Check if measurements are registered");
    if (service != null) {
      service.getUnregisteredTimeseries(header, timeseriesList, resultHandler);
    }
  }

  @Override
  public void getGroupByExecutor(GroupByRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.getGroupByExecutor(request, resultHandler);
    }
  }

  @Override
  public void getGroupByResult(
      RaftNode header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "Fetch group by");
    if (service != null) {
      service.getGroupByResult(header, executorId, startTime, endTime, resultHandler);
    }
  }

  @Override
  public void previousFill(
      PreviousFillRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, request);
    if (service != null) {
      service.previousFill(request, resultHandler);
    }
  }

  @Override
  public void matchTerm(
      long index, long term, RaftNode header, AsyncMethodCallback<Boolean> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "Match term");
    if (service != null) {
      service.matchTerm(index, term, header, resultHandler);
    }
  }

  @Override
  public void last(LastQueryRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(request.getHeader(), resultHandler, "last");
    if (service != null) {
      service.last(request, resultHandler);
    }
  }

  @Override
  public void getPathCount(
      RaftNode header,
      List<String> pathsToQuery,
      int level,
      AsyncMethodCallback<Integer> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "count path");
    if (service != null) {
      service.getPathCount(header, pathsToQuery, level, resultHandler);
    }
  }

  @Override
  public void getDeviceCount(
      RaftNode header, List<String> pathsToQuery, AsyncMethodCallback<Integer> resultHandler)
      throws TException {
    DataAsyncService service =
        DataGroupEngine.getInstance().getDataAsyncService(header, resultHandler, "count device");
    if (service != null) {
      service.getDeviceCount(header, pathsToQuery, resultHandler);
    }
  }

  @Override
  public void onSnapshotApplied(
      RaftNode header, List<Integer> slots, AsyncMethodCallback<Boolean> resultHandler) {
    DataAsyncService service =
        DataGroupEngine.getInstance()
            .getDataAsyncService(header, resultHandler, "Snapshot applied");
    if (service != null) {
      service.onSnapshotApplied(header, slots, resultHandler);
    }
  }

  @Override
  public long querySingleSeries(SingleSeriesQueryRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .querySingleSeries(request);
  }

  @Override
  public long queryMultSeries(MultSeriesQueryRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .queryMultSeries(request);
  }

  @Override
  public ByteBuffer fetchSingleSeries(RaftNode header, long readerId) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .fetchSingleSeries(header, readerId);
  }

  @Override
  public Map<String, ByteBuffer> fetchMultSeries(RaftNode header, long readerId, List<String> paths)
      throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .fetchMultSeries(header, readerId, paths);
  }

  @Override
  public long querySingleSeriesByTimestamp(SingleSeriesQueryRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .querySingleSeriesByTimestamp(request);
  }

  @Override
  public ByteBuffer fetchSingleSeriesByTimestamps(
      RaftNode header, long readerId, List<Long> timestamps) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .fetchSingleSeriesByTimestamps(header, readerId, timestamps);
  }

  @Override
  public void endQuery(RaftNode header, Node thisNode, long queryId) throws TException {
    DataGroupEngine.getInstance().getDataSyncService(header).endQuery(header, thisNode, queryId);
  }

  @Override
  public GetAllPathsResult getAllPaths(RaftNode header, List<String> path, boolean withAlias)
      throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getAllPaths(header, path, withAlias);
  }

  @Override
  public Set<String> getAllDevices(RaftNode header, List<String> path) throws TException {
    return DataGroupEngine.getInstance().getDataSyncService(header).getAllDevices(header, path);
  }

  @Override
  public List<String> getNodeList(RaftNode header, String path, int nodeLevel) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getNodeList(header, path, nodeLevel);
  }

  @Override
  public Set<String> getChildNodeInNextLevel(RaftNode header, String path) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getChildNodeInNextLevel(header, path);
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(RaftNode header, String path) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getChildNodePathInNextLevel(header, path);
  }

  @Override
  public ByteBuffer getAllMeasurementSchema(RaftNode header, ByteBuffer planBinary)
      throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getAllMeasurementSchema(header, planBinary);
  }

  @Override
  public ByteBuffer getDevices(RaftNode header, ByteBuffer planBinary) throws TException {
    return DataGroupEngine.getInstance().getDataSyncService(header).getDevices(header, planBinary);
  }

  @Override
  public List<ByteBuffer> getAggrResult(GetAggrResultRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .getAggrResult(request);
  }

  @Override
  public List<String> getUnregisteredTimeseries(RaftNode header, List<String> timeseriesList)
      throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getUnregisteredTimeseries(header, timeseriesList);
  }

  @Override
  public PullSnapshotResp pullSnapshot(PullSnapshotRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .pullSnapshot(request);
  }

  @Override
  public long getGroupByExecutor(GroupByRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .getGroupByExecutor(request);
  }

  @Override
  public List<ByteBuffer> getGroupByResult(
      RaftNode header, long executorId, long startTime, long endTime) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getGroupByResult(header, executorId, startTime, endTime);
  }

  @Override
  public PullSchemaResp pullTimeSeriesSchema(PullSchemaRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .pullTimeSeriesSchema(request);
  }

  @Override
  public PullSchemaResp pullMeasurementSchema(PullSchemaRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .pullMeasurementSchema(request);
  }

  @Override
  public ByteBuffer previousFill(PreviousFillRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .previousFill(request);
  }

  @Override
  public ByteBuffer last(LastQueryRequest request) throws TException {
    return DataGroupEngine.getInstance().getDataSyncService(request.getHeader()).last(request);
  }

  @Override
  public int getPathCount(RaftNode header, List<String> pathsToQuery, int level) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getPathCount(header, pathsToQuery, level);
  }

  @Override
  public boolean onSnapshotApplied(RaftNode header, List<Integer> slots) {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .onSnapshotApplied(header, slots);
  }

  @Override
  public int getDeviceCount(RaftNode header, List<String> pathsToQuery) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .getDeviceCount(header, pathsToQuery);
  }

  @Override
  public HeartBeatResponse sendHeartbeat(HeartBeatRequest request) {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .sendHeartbeat(request);
  }

  @Override
  public long startElection(ElectionRequest request) {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .startElection(request);
  }

  @Override
  public long appendEntries(AppendEntriesRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .appendEntries(request);
  }

  @Override
  public long appendEntry(AppendEntryRequest request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .appendEntry(request);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request) throws TException {
    DataGroupEngine.getInstance().getDataSyncService(request.getHeader()).sendSnapshot(request);
  }

  @Override
  public TSStatus executeNonQueryPlan(ExecutNonQueryReq request) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(request.getHeader())
        .executeNonQueryPlan(request);
  }

  @Override
  public RequestCommitIndexResponse requestCommitIndex(RaftNode header) throws TException {
    return DataGroupEngine.getInstance().getDataSyncService(header).requestCommitIndex(header);
  }

  @Override
  public ByteBuffer readFile(String filePath, long offset, int length) throws TException {
    try {
      return IOUtils.readFile(filePath, offset, length);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean matchTerm(long index, long term, RaftNode header) {
    return DataGroupEngine.getInstance().getDataSyncService(header).matchTerm(index, term, header);
  }

  @Override
  public ByteBuffer peekNextNotNullValue(
      RaftNode header, long executorId, long startTime, long endTime) throws TException {
    return DataGroupEngine.getInstance()
        .getDataSyncService(header)
        .peekNextNotNullValue(header, executorId, startTime, endTime);
  }

  @Override
  public void peekNextNotNullValue(
      RaftNode header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<ByteBuffer> resultHandler)
      throws TException {
    resultHandler.onComplete(
        DataGroupEngine.getInstance()
            .getDataSyncService(header)
            .peekNextNotNullValue(header, executorId, startTime, endTime));
  }

  @Override
  public void removeHardLink(String hardLinkPath) throws TException {
    try {
      Files.deleteIfExists(new File(hardLinkPath).toPath());
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public void removeHardLink(String hardLinkPath, AsyncMethodCallback<Void> resultHandler) {
    try {
      Files.deleteIfExists(new File(hardLinkPath).toPath());
      resultHandler.onComplete(null);
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }
}
