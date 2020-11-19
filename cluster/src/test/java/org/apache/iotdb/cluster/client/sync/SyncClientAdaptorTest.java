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

package org.apache.iotdb.cluster.client.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class SyncClientAdaptorTest {

  private AsyncMetaClient metaClient;
  private AsyncDataClient dataClient;

  private TNodeStatus nodeStatus;
  private CheckStatusResponse checkStatusResponse;
  private AddNodeResponse addNodeResponse;
  private List<ByteBuffer> aggregateResults;
  private ByteBuffer getAllMeasurementSchemaResult;
  private ByteBuffer fillResult;
  private ByteBuffer readFileResult;
  private ByteBuffer peekNextNotNullValueResult;
  private Map<Integer, SimpleSnapshot> snapshotMap;
  private ByteBuffer lastResult;
  private List<MeasurementSchema> measurementSchemas;
  private List<TimeseriesSchema> timeseriesSchemas;
  private List<String> paths;

  @Before
  public void setUp() {
    nodeStatus = new TNodeStatus();
    checkStatusResponse = new CheckStatusResponse(true, false, true, false, true);
    addNodeResponse = new AddNodeResponse((int) Response.RESPONSE_AGREE);
    aggregateResults = Arrays.asList(ByteBuffer.wrap("1".getBytes()),
        ByteBuffer.wrap("2".getBytes()), ByteBuffer.wrap("2".getBytes()));
    getAllMeasurementSchemaResult = ByteBuffer.wrap("get all measurement schema".getBytes());
    fillResult = ByteBuffer.wrap("fill result".getBytes());
    readFileResult = ByteBuffer.wrap("read file".getBytes());
    peekNextNotNullValueResult = ByteBuffer.wrap("peek next not null value".getBytes());
    measurementSchemas = new ArrayList<>();
    timeseriesSchemas = new ArrayList<>();
    snapshotMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      snapshotMap.put(i, new SimpleSnapshot(i, i));
      measurementSchemas.add(new MeasurementSchema(String.valueOf(i), TSDataType.INT64));
      timeseriesSchemas.add(new TimeseriesSchema(String.valueOf(i), TSDataType.INT64));
    }
    lastResult = ByteBuffer.wrap("last".getBytes());
    paths = Arrays.asList("1", "2", "3", "4");

    metaClient = new AsyncMetaClient(null, null, null) {
      @Override
      public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
        resultHandler.onComplete(Response.RESPONSE_AGREE);
      }

      @Override
      public void matchTerm(long index, long term, Node header,
          AsyncMethodCallback<Boolean> resultHandler) {
        resultHandler.onComplete(true);
      }

      @Override
      public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
        resultHandler.onComplete(nodeStatus);
      }

      @Override
      public void checkStatus(StartUpStatus startUpStatus,
          AsyncMethodCallback<CheckStatusResponse> resultHandler) {
        resultHandler.onComplete(checkStatusResponse);
      }

      @Override
      public void addNode(Node node, StartUpStatus startUpStatus,
          AsyncMethodCallback<AddNodeResponse> resultHandler) {
        resultHandler.onComplete(addNodeResponse);
      }

      @Override
      public void executeNonQueryPlan(ExecutNonQueryReq request,
          AsyncMethodCallback<TSStatus> resultHandler) {
        resultHandler.onComplete(StatusUtils.OK);
      }
    };

    dataClient = new AsyncDataClient(null, null, null) {
      @Override
      public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
          AsyncMethodCallback<Long> resultHandler) {
        resultHandler.onComplete(1L);
      }

      @Override
      public void querySingleSeries(SingleSeriesQueryRequest request,
          AsyncMethodCallback<Long> resultHandler) {
        resultHandler.onComplete(1L);
      }

      @Override
      public void getNodeList(Node header, String path, int nodeLevel,
          AsyncMethodCallback<List<String>> resultHandler) {
        resultHandler.onComplete(Arrays.asList("1", "2", "3"));
      }

      @Override
      public void getChildNodePathInNextLevel(Node header, String path,
          AsyncMethodCallback<Set<String>> resultHandler) {
        resultHandler.onComplete(new HashSet<>(Arrays.asList("1", "2", "3")));
      }

      @Override
      public void getAllMeasurementSchema(Node header, ByteBuffer planBinary,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        resultHandler.onComplete(getAllMeasurementSchemaResult);
      }

      @Override
      public void pullMeasurementSchema(PullSchemaRequest request,
          AsyncMethodCallback<PullSchemaResp> resultHandler) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
        byteBuffer.putInt(measurementSchemas.size());
        for (MeasurementSchema schema : measurementSchemas) {
          schema.serializeTo(byteBuffer);
        }
        byteBuffer.flip();
        resultHandler.onComplete(new PullSchemaResp(byteBuffer));
      }

      @Override
      public void pullTimeSeriesSchema(PullSchemaRequest request,
          AsyncMethodCallback<PullSchemaResp> resultHandler) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
        byteBuffer.putInt(timeseriesSchemas.size());
        for (TimeseriesSchema schema : timeseriesSchemas) {
          schema.serializeTo(byteBuffer);
        }
        byteBuffer.flip();
        resultHandler.onComplete(new PullSchemaResp(byteBuffer));
      }

      @Override
      public void getAggrResult(GetAggrResultRequest request,
          AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
        resultHandler.onComplete(aggregateResults);
      }

      @Override
      public void getUnregisteredTimeseries(Node header, List<String> timeseriesList,
          AsyncMethodCallback<List<String>> resultHandler) {
        resultHandler.onComplete(timeseriesList.subList(0, timeseriesList.size() / 2));
      }

      @Override
      public void getAllPaths(Node header, List<String> path, boolean withAlias,
          AsyncMethodCallback<GetAllPathsResult> resultHandler) {
        resultHandler.onComplete(new GetAllPathsResult(path));
      }

      @Override
      public void getPathCount(Node header, List<String> pathsToQuery, int level,
          AsyncMethodCallback<Integer> resultHandler) {
        resultHandler.onComplete(pathsToQuery.size());
      }

      @Override
      public void getAllDevices(Node header, List<String> path,
          AsyncMethodCallback<Set<String>> resultHandler) {
        resultHandler.onComplete(new HashSet<>(path));
      }

      @Override
      public void getGroupByExecutor(GroupByRequest request,
          AsyncMethodCallback<Long> resultHandler) {
        resultHandler.onComplete(1L);
      }

      @Override
      public void previousFill(PreviousFillRequest request,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        resultHandler.onComplete(fillResult);
      }

      @Override
      public void readFile(String filePath, long offset, int length,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        resultHandler.onComplete(readFileResult);
      }

      @Override
      public void getGroupByResult(Node header, long executorId, long startTime, long endTime,
          AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
        resultHandler.onComplete(aggregateResults);
      }

      @Override
      public void peekNextNotNullValue(Node header, long executorId, long startTime, long endTime,
          AsyncMethodCallback<ByteBuffer> resultHandler) {
        resultHandler.onComplete(peekNextNotNullValueResult);
      }

      @Override
      public void pullSnapshot(PullSnapshotRequest request,
          AsyncMethodCallback<PullSnapshotResp> resultHandler) {
        Map<Integer, ByteBuffer> snapshotBytes = new HashMap<>();
        for (Entry<Integer, SimpleSnapshot> integerSimpleSnapshotEntry : snapshotMap.entrySet()) {
          snapshotBytes.put(integerSimpleSnapshotEntry.getKey(),
              integerSimpleSnapshotEntry.getValue().serialize());
        }
        PullSnapshotResp pullSnapshotResp = new PullSnapshotResp();
        pullSnapshotResp.snapshotBytes = snapshotBytes;
        resultHandler.onComplete(pullSnapshotResp);
      }

      @Override
      public void last(LastQueryRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
        resultHandler.onComplete(lastResult);
      }

      @Override
      public void onSnapshotApplied(Node header, List<Integer> slots,
          AsyncMethodCallback<Boolean> resultHandler) {
        resultHandler.onComplete(true);
      }
    };
  }

  @Test
  public void testMetaClient()
      throws TException, InterruptedException, IOException {
    assertEquals(Response.RESPONSE_AGREE, (long) SyncClientAdaptor.removeNode(metaClient,
        TestUtils.getNode(0)));
    assertTrue(SyncClientAdaptor.matchTerm(metaClient, TestUtils.getNode(0), 1, 1,
        TestUtils.getNode(0)));
    assertEquals(nodeStatus, SyncClientAdaptor.queryNodeStatus(metaClient));
    assertEquals(checkStatusResponse,
        SyncClientAdaptor.checkStatus(metaClient, new StartUpStatus()));
    assertEquals(addNodeResponse, SyncClientAdaptor.addNode(metaClient, TestUtils.getNode(0),
        new StartUpStatus()));
    assertEquals(StatusUtils.OK, SyncClientAdaptor.executeNonQuery(metaClient, new FlushPlan(),
        TestUtils.getNode(0), TestUtils.getNode(1)));
  }


  @Test
  public void testDataClient()
      throws TException, InterruptedException, IOException, IllegalPathException {
    assertEquals(1L, (long) SyncClientAdaptor.querySingleSeriesByTimestamp(dataClient,
        new SingleSeriesQueryRequest()));
    assertEquals(1L, (long) SyncClientAdaptor.querySingleSeries(dataClient,
        new SingleSeriesQueryRequest(), 0));
    assertEquals(Arrays.asList("1", "2", "3"), SyncClientAdaptor.getNodeList(dataClient,
        TestUtils.getNode(0), "root", 0));
    assertEquals(new HashSet<>(Arrays.asList("1", "2", "3")),
        SyncClientAdaptor.getNextChildren(dataClient, TestUtils.getNode(0), "root"));
    assertEquals(getAllMeasurementSchemaResult,
        SyncClientAdaptor.getAllMeasurementSchema(dataClient, TestUtils.getNode(0),
            new ShowTimeSeriesPlan(new PartialPath("root"))));
    assertEquals(measurementSchemas, SyncClientAdaptor.pullMeasurementSchema(dataClient,
        new PullSchemaRequest()));
    assertEquals(timeseriesSchemas, SyncClientAdaptor.pullTimeseriesSchema(dataClient,
        new PullSchemaRequest()));
    assertEquals(aggregateResults, SyncClientAdaptor.getAggrResult(dataClient
        , new GetAggrResultRequest()));
    assertEquals(paths.subList(0, paths.size() / 2),
        SyncClientAdaptor.getUnregisteredMeasurements(dataClient, TestUtils.getNode(0), paths));
    assertEquals(paths, SyncClientAdaptor.getAllPaths(dataClient, TestUtils.getNode(0), paths,
        false).paths);
    assertEquals(paths.size(), (int) SyncClientAdaptor.getPathCount(dataClient,
        TestUtils.getNode(0),
        paths, 0));
    assertEquals(new HashSet<>(paths), SyncClientAdaptor.getAllDevices(dataClient,
        TestUtils.getNode(0), paths));
    assertEquals(1L, (long) SyncClientAdaptor.getGroupByExecutor(dataClient, new GroupByRequest()));
    assertEquals(fillResult, SyncClientAdaptor.previousFill(dataClient, new PreviousFillRequest()));
    assertEquals(readFileResult, SyncClientAdaptor.readFile(dataClient, "a file", 0, 1000));
    assertEquals(aggregateResults, SyncClientAdaptor.getGroupByResult(dataClient,
        TestUtils.getNode(0), 1, 1, 2));
    assertEquals(peekNextNotNullValueResult, SyncClientAdaptor.peekNextNotNullValue(dataClient,
        TestUtils.getNode(0), 1, 1, 1));
    assertEquals(snapshotMap, SyncClientAdaptor.pullSnapshot(dataClient,
        new PullSnapshotRequest(), Arrays.asList(0, 1, 2),
        new SnapshotFactory<Snapshot>() {
          @Override
          public Snapshot create() {
            return new SimpleSnapshot(0, 0);
          }

          @Override
          public Snapshot copy(Snapshot origin) {
            return new SimpleSnapshot(0, 0);
          }
        }));
    assertEquals(lastResult, SyncClientAdaptor.last(dataClient,
        Collections.singletonList(new PartialPath("1")),
        Collections.singletonList(TSDataType.INT64.ordinal()),
        new QueryContext(), Collections.emptyMap(), TestUtils.getNode(0)));
    assertTrue(SyncClientAdaptor.onSnapshotApplied(dataClient, TestUtils.getNode(0),
        Arrays.asList(0, 1, 2)));
  }
}