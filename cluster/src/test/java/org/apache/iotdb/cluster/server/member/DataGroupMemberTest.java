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

package org.apache.iotdb.cluster.server.member;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.RemoteFileSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataGroupMemberTest extends MemberTest {

  private DataGroupMember dataGroupMember;
  private DataGroupMember.Factory factory;
  private Map<Integer, FileSnapshot> snapshotMap;
  private Map<Integer, RemoteFileSnapshot> receivedSnapshots;
  private boolean hasInitialSnapshots;
  private boolean enableSyncLeader;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataGroupMember = getDataGroupMember(TestUtils.getNode(0));
    snapshotMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      fileSnapshot.setTimeseriesSchemas(Collections.singleton(TestUtils.getTestSchema(1, i)));
      snapshotMap.put(i, fileSnapshot);
    }
    receivedSnapshots = new HashMap<>();
  }

  private PartitionedSnapshotLogManager getLogManager() {
    return new TestPartitionedLogManager(new DataLogApplier(testMetaMember),
        testMetaMember.getPartitionTable(), partitionGroup.getHeader(), FileSnapshot::new) {
      @Override
      public Snapshot getSnapshot() {
        PartitionedSnapshot<FileSnapshot> snapshot = new PartitionedSnapshot<>(FileSnapshot::new);
        if (hasInitialSnapshots) {
          for (int i = 0; i < 100; i++) {
            snapshot.putSnapshot(i, snapshotMap.get(i));
          }
        }
        return snapshot;
      }

      @Override
      public void setSnapshot(Snapshot snapshot, int slot) {
        receivedSnapshots.put(slot, (RemoteFileSnapshot) snapshot);
      }
    };
  }

  private DataGroupMember getDataGroupMember(Node node) throws IOException {
    return new DataGroupMember(new TCompactProtocol.Factory(), new PartitionGroup(partitionGroup)
        , node, getLogManager(),
        testMetaMember, new TAsyncClientManager()) {
      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestDataClient(node) {
            @Override
            public void readFile(String filePath, long offset, int length, Node header,
                AsyncMethodCallback<ByteBuffer> resultHandler) {
              new Thread(() -> {
                if (offset == 0) {
                  resultHandler.onComplete(
                          ByteBuffer.wrap((filePath + "@" + offset + "#" + length).getBytes()));
                } else {
                  resultHandler.onComplete(ByteBuffer.allocate(0));
                }
              }).start();
            }

            @Override
            public void startElection(ElectionRequest request,
                AsyncMethodCallback<Long> resultHandler) {
            }

            @Override
            public void pullSnapshot(PullSnapshotRequest request,
                AsyncMethodCallback<PullSnapshotResp> resultHandler) {
              new Thread(() -> {
                PullSnapshotResp resp = new PullSnapshotResp();
                Map<Integer, ByteBuffer> snapshotBufferMap = new HashMap<>();
                for (Integer requiredSlot : request.getRequiredSlots()) {
                  snapshotBufferMap.put(requiredSlot, snapshotMap.get(requiredSlot).serialize());
                }
                resp.setSnapshotBytes(snapshotBufferMap);
                resultHandler.onComplete(resp);
              }).start();
            }

            @Override
            public void executeNonQueryPlan(ExecutNonQueryReq request,
                AsyncMethodCallback<TSStatus> resultHandler) {
              new Thread(() -> {
                try {
                  PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
                  new QueryProcessExecutor().processNonQuery(plan);
                  resultHandler.onComplete(StatusUtils.OK);
                } catch (IOException | QueryProcessException e) {
                  resultHandler.onError(e);
                }
              }).start();
            }

            @Override
            public void appendEntry(AppendEntryRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> resultHandler.onComplete(Response.RESPONSE_AGREE)).start();
            }

            @Override
            public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                if (enableSyncLeader) {
                  resultHandler.onComplete(-1L);
                } else {
                  resultHandler.onError(new TestException());
                }
              }).start();
            }

            @Override
            public void pullTimeSeriesSchema(PullSchemaRequest request,
                AsyncMethodCallback<PullSchemaResp> resultHandler) {
              new Thread(() -> {
                PullSchemaResp resp;
                try {
                  resp = new PullSchemaResp();
                  ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                  DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                  dataOutputStream.writeInt(10);
                  for (int i = 0; i < 10; i++) {
                    TestUtils.getTestSchema(0, i).serializeTo(dataOutputStream);
                  }
                  resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
                  resultHandler.onComplete(resp);
                } catch (IOException e) {
                  resultHandler.onError(e);
                }
              }).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testGetHeader() {
    assertEquals(TestUtils.getNode(0), dataGroupMember.getHeader());
  }

  @Test
  public void testAddNode() throws IOException {
    DataGroupMember firstMember = dataGroupMember;
    DataGroupMember midMember = getDataGroupMember(TestUtils.getNode(50));
    DataGroupMember lastMember = getDataGroupMember(TestUtils.getNode(90));

    Node newNodeBeforeGroup = TestUtils.getNode(-5);
    assertFalse(firstMember.addNode(newNodeBeforeGroup));
    assertFalse(midMember.addNode(newNodeBeforeGroup));
    assertFalse(lastMember.addNode(newNodeBeforeGroup));

    Node newNodeInGroup = TestUtils.getNode(66);
    assertFalse(firstMember.addNode(newNodeInGroup));
    assertFalse(midMember.addNode(newNodeInGroup));
    assertTrue(lastMember.addNode(newNodeInGroup));

    Node newNodeAfterGroup = TestUtils.getNode(101);
    assertFalse(firstMember.addNode(newNodeInGroup));
    assertFalse(midMember.addNode(newNodeInGroup));
  }

  @Test
  public void testProcessElectionRequest() {
    dataGroupMember.getLogManager().setLastLogId(10);
    dataGroupMember.getLogManager().setLastLogTerm(10);
    dataGroupMember.getTerm().set(10);
    testMetaMember.getTerm().set(10);
    metaLogManager.setLastLogId(5);
    metaLogManager.setLastLogTerm(5);

    // a valid request
    ElectionRequest electionRequest = new ElectionRequest();
    electionRequest.setTerm(11);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(100);
    electionRequest.setDataLogLastIndex(100);
    assertEquals(Response.RESPONSE_AGREE, dataGroupMember.processElectionRequest(electionRequest));

    // a request with too small term
    electionRequest.setTerm(1);
    assertEquals(11, dataGroupMember.processElectionRequest(electionRequest));

    // a request with stale meta log
    electionRequest.setTerm(11);
    electionRequest.setLastLogIndex(1);
    electionRequest.setLastLogTerm(1);
    assertEquals(Response.RESPONSE_META_LOG_STALE,
        dataGroupMember.processElectionRequest(electionRequest));

    // a request with stale data log
    electionRequest.setTerm(12);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(1);
    electionRequest.setDataLogLastIndex(1);
    assertEquals(Response.RESPONSE_LOG_MISMATCH,
        dataGroupMember.processElectionRequest(electionRequest));
  }

  @Test
  public void testSendSnapshot() {
    PartitionedSnapshot<FileSnapshot> partitionedSnapshot =
        new PartitionedSnapshot<>(FileSnapshot::new);
    partitionedSnapshot.setLastLogId(100);
    partitionedSnapshot.setLastLogTerm(100);

    for (int i = 0; i < 3; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      partitionedSnapshot.putSnapshot(i, fileSnapshot);
    }
    ByteBuffer serialize = partitionedSnapshot.serialize();

    SendSnapshotRequest request = new SendSnapshotRequest();
    request.setSnapshotBytes(serialize);
    AtomicBoolean callbackCalled = new AtomicBoolean(false);
    dataGroupMember.sendSnapshot(request, new AsyncMethodCallback() {
      @Override
      public void onComplete(Object o) {
        callbackCalled.set(true);
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    });

    assertTrue(callbackCalled.get());
    assertEquals(100, dataGroupMember.getLogManager().getLastLogIndex());
    assertEquals(100, dataGroupMember.getLogManager().getLastLogTerm());
  }

  @Test
  public void testApplySnapshot()
      throws StorageEngineException, QueryProcessException, IOException {
    FileSnapshot snapshot = new FileSnapshot();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      schemaList.add(TestUtils.getTestSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(schemaList);

    // resource1 exists locally, resource2 does not exist locally and without modification,
    // resource2 does not exist locally and with modification
    snapshot.addFile(prepareResource(0, false), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(1, false), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(2, true), TestUtils.getNode(0));

    // create a local resource1
    StorageGroupProcessor processor = StorageEngine.getInstance()
        .getProcessor(TestUtils.getTestSg(0));
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setTime(0);
    insertPlan.setMeasurements(new String[]{"s0"});
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.DOUBLE});
    insertPlan.setValues(new String[]{"1.0"});
    processor.insert(insertPlan);
    processor.waitForAllCurrentTsFileProcessorsClosed();

    dataGroupMember.applySnapshot(snapshot, 0);
    assertEquals(3, processor.getSequenceFileTreeSet().size());
    assertEquals(0, processor.getUnSequenceFileList().size());
    Deletion deletion = new Deletion(new Path(TestUtils.getTestSg(0)), 0, 0);
    assertTrue(processor.getSequenceFileTreeSet().get(2).getModFile().getModifications()
        .contains(deletion));
  }

  @Test
  public void testForwardPullSnapshot() throws InterruptedException {
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler = new PullSnapshotHandler<>(reference,
        TestUtils.getNode(1), request.getRequiredSlots(), FileSnapshot::new);
    synchronized (reference) {
      dataGroupMember.pullSnapshot(request, handler);
      reference.wait(500);
    }
    assertEquals(requiredSlots.size(), reference.get().size());
    for (Integer requiredSlot : requiredSlots) {
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testPullSnapshot() throws InterruptedException {
    hasInitialSnapshots = true;
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9, 11);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler = new PullSnapshotHandler<>(reference,
        TestUtils.getNode(1), request.getRequiredSlots(), FileSnapshot::new);
    synchronized (reference) {
      dataGroupMember.pullSnapshot(request, handler);
      reference.wait( 500);
    }
    assertEquals(requiredSlots.size() - 1, reference.get().size());
    for (int i = 0; i < requiredSlots.size() - 1; i++) {
      Integer requiredSlot = requiredSlots.get(i);
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testPullRemoteSnapshot() throws TTransportException {
    dataGroupMember.start();
    try {
      hasInitialSnapshots = false;
      partitionTable.addNode(TestUtils.getNode(10));
      List<Integer> requiredSlots = Arrays.asList(19, 39, 59, 79, 99);
      dataGroupMember.pullNodeAdditionSnapshots(requiredSlots, TestUtils.getNode(10));
      assertEquals(requiredSlots.size(), receivedSnapshots.size());
      for (Integer requiredSlot : requiredSlots) {
        receivedSnapshots.get(requiredSlot).getRemoteSnapshot();
        assertTrue(MManager.getInstance().pathExist(TestUtils.getTestSeries(1, requiredSlot)));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testFollowerExecuteNonQuery() {
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    MeasurementSchema measurementSchema = TestUtils.getTestSchema(2, 0);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(new Path(measurementSchema.getMeasurementId()),
            measurementSchema.getType(), measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(), measurementSchema.getProps());
    assertEquals(200, dataGroupMember.executeNonQuery(createTimeSeriesPlan).statusType.code);
    assertTrue(MManager.getInstance().pathExist(measurementSchema.getMeasurementId()));
  }

  @Test
  public void testLeaderExecuteNonQuery() {
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    MeasurementSchema measurementSchema = TestUtils.getTestSchema(2, 0);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(new Path(measurementSchema.getMeasurementId()),
            measurementSchema.getType(), measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(), measurementSchema.getProps());
    assertEquals(200, dataGroupMember.executeNonQuery(createTimeSeriesPlan).statusType.code);
    assertTrue(MManager.getInstance().pathExist(measurementSchema.getMeasurementId()));
  }

  @Test
  public void testPullTimeseries() throws InterruptedException {
    int prevTimeOut = RaftServer.connectionTimeoutInMS;
    int prevMaxWait = RaftServer.syncLeaderMaxWaitMs;
    RaftServer.connectionTimeoutInMS = 20;
    RaftServer.syncLeaderMaxWaitMs = 200;
    try {
      // sync with leader is temporarily disabled, the request should be forward to the leader
      dataGroupMember.setLeader(TestUtils.getNode(1));
      dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
      enableSyncLeader = false;

      PullSchemaRequest request = new PullSchemaRequest();
      request.setPrefixPath(TestUtils.getTestSg(0));
      AtomicReference<List<MeasurementSchema>> result = new AtomicReference<>();
      PullTimeseriesSchemaHandler handler = new PullTimeseriesSchemaHandler(TestUtils.getNode(1),
          request.getPrefixPath(), result);
      synchronized (result) {
        dataGroupMember.pullTimeSeriesSchema(request, handler);
        result.wait(500);
      }
      for (int i = 0; i < 10; i++) {
        assertEquals(TestUtils.getTestSchema(0, i), result.get().get(i));
      }

      // the member is a leader itself
      dataGroupMember.setCharacter(NodeCharacter.LEADER);
      result.set(null);
      handler = new PullTimeseriesSchemaHandler(TestUtils.getNode(1),
          request.getPrefixPath(), result);
      synchronized (result) {
        dataGroupMember.pullTimeSeriesSchema(request, handler);
        result.wait(500);
      }
      for (int i = 0; i < 10; i++) {
        assertEquals(TestUtils.getTestSchema(0, i), result.get().get(i));
      }
    } finally {
      RaftServer.connectionTimeoutInMS = prevTimeOut;
      RaftServer.syncLeaderMaxWaitMs = prevMaxWait;
    }
  }

  @Test
  public void testQuerySingleSeries() throws QueryProcessException, InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      QueryProcessExecutor queryProcessExecutor = new QueryProcessExecutor();
      queryProcessExecutor.processNonQuery(insertPlan);
    }

    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setWithValueFilter(false);
    request.setPushdownUnseq(true);
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = TimeFilter.gtEq(5);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeries(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    synchronized (dataResult) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
      dataResult.wait(200);
    }
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 10; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesWithValueFilter() throws QueryProcessException,
      InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      QueryProcessExecutor queryProcessExecutor = new QueryProcessExecutor();
      queryProcessExecutor.processNonQuery(insertPlan);
    }

    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setWithValueFilter(true);
    request.setPushdownUnseq(true);
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = new AndFilter(TimeFilter.gtEq(5), ValueFilter.ltEq(8.0));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeries(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    synchronized (dataResult) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
      dataResult.wait(200);
    }
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 9; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestamp() throws QueryProcessException, InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      QueryProcessExecutor queryProcessExecutor = new QueryProcessExecutor();
      queryProcessExecutor.processNonQuery(insertPlan);
    }

    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setWithValueFilter(false);
    request.setPushdownUnseq(true);
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = TimeFilter.gtEq(5);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeriesByTimestamp(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    for (int i = 5; i < 10; i++) {
      dataResult.set(null);
      synchronized (dataResult) {
        dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), readerId, i,
            dataHandler);
        dataResult.wait(200);
      }
      double value = (double) SerializeUtils.deserializeObject(dataResult.get());
      assertEquals(i * 1.0, value, 0.00001);
    }

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestampWithValueFilter() throws QueryProcessException,
      InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      QueryProcessExecutor queryProcessExecutor = new QueryProcessExecutor();
      queryProcessExecutor.processNonQuery(insertPlan);
    }

    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setWithValueFilter(true);
    request.setPushdownUnseq(true);
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = new AndFilter(TimeFilter.gtEq(5), ValueFilter.ltEq(8.0));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeriesByTimestamp(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    for (int i = 5; i < 9; i++) {
      dataResult.set(null);
      synchronized (dataResult) {
        dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), readerId, i,
            dataHandler);
        dataResult.wait(200);
      }
      double value = (double) SerializeUtils.deserializeObject(dataResult.get());
      assertEquals(i * 1.0, value, 0.00001);
    }

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testGetPaths() throws InterruptedException {
    String path = TestUtils.getTestSg(0);
    AtomicReference<List<String>> pathResult = new AtomicReference<>();
    GenericHandler<List<String>> handler = new GenericHandler<>(TestUtils.getNode(0), pathResult);
    synchronized (pathResult) {
      dataGroupMember.getAllPaths(TestUtils.getNode(0), path, handler);
      pathResult.wait(200);
    }
    List<String> result = pathResult.get();
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(TestUtils.getTestSeries(0, i), result.get(i));
    }
  }

  @Test
  public void testFetchWithoutQuery() throws InterruptedException {
    AtomicReference<Exception> result = new AtomicReference<>();
    synchronized (result) {
      dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), 0, 0,
          new AsyncMethodCallback<ByteBuffer>() {
            @Override
            public void onComplete(ByteBuffer buffer) {
              new Thread(() -> {
                synchronized (result) {
                  result.notifyAll();
                }
              }).start();
            }

            @Override
            public void onError(Exception e) {
              new Thread(() -> {
                synchronized (result) {
                  result.set(e);
                  result.notifyAll();
                }
              }).start();
            }
          });
      result.wait(200);
    }
    Exception exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());

    synchronized (result) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), 0,
          new AsyncMethodCallback<ByteBuffer>() {
            @Override
            public void onComplete(ByteBuffer buffer) {
              new Thread(() -> {
                synchronized (result) {
                  result.notifyAll();
                }
              }).start();
            }

            @Override
            public void onError(Exception e) {
              new Thread(() -> {
                synchronized (result) {
                  result.set(e);
                  result.notifyAll();
                }
              }).start();
            }
          });
      result.wait(200);
    }
    exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());
  }

  private TsFileResource prepareResource(int serialNum, boolean withModification)
      throws IOException {
    TsFileResource resource = new RemoteTsFileResource();
    File file = new File("target" + File.separator + TestUtils.getTestSg(0),
        "0-" + (serialNum + 101L) + "-0.tsfile");
    file.getParentFile().mkdirs();
    file.createNewFile();

    resource.setFile(file);
    resource.setHistoricalVersions(Collections.singleton(serialNum + 101L));
    resource.updateStartTime(TestUtils.getTestSg(0), serialNum * 100);
    resource.updateEndTime(TestUtils.getTestSg(0), (serialNum + 1) * 100 - 1);
    if (withModification) {
      Deletion deletion = new Deletion(new Path(TestUtils.getTestSg(0)), 0, 0);
      resource.getModFile().write(deletion);
      resource.getModFile().close();
    }
    return resource;
  }

}