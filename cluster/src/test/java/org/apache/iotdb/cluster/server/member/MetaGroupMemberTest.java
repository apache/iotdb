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

import static org.apache.iotdb.cluster.server.NodeCharacter.ELECTOR;
import static org.apache.iotdb.cluster.server.NodeCharacter.FOLLOWER;
import static org.apache.iotdb.cluster.server.NodeCharacter.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

public class MetaGroupMemberTest extends MemberTest {

  private MetaGroupMember metaGroupMember;
  private DataClusterServer dataClusterServer;
  private AtomicLong dummyResponse;
  private boolean mockDataClusterServer;
  private Node exiledNode;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dummyResponse = new AtomicLong(Response.RESPONSE_AGREE);
    metaGroupMember = getMetaGroupMember(TestUtils.getNode(0));
    metaGroupMember.setAllNodes(allNodes);
    // a faked data member to respond requests
    DataGroupMember dataGroupMember = getDataGroupMember(allNodes, TestUtils.getNode(0));
    dataGroupMember.setCharacter(LEADER);
    dataClusterServer = new DataClusterServer(TestUtils.getNode(0),
        new DataGroupMember.Factory(null, metaGroupMember, null) {
          @Override
          public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
            return getDataGroupMember(partitionGroup, thisNode);
          }
        }) {
    };
    buildDataGroups(dataClusterServer);
    metaGroupMember.setPartitionTable(partitionTable);
    metaGroupMember.getThisNode().setNodeIdentifier(0);
    mockDataClusterServer = false;
    QueryCoordinator.getINSTANCE().setMetaGroupMember(metaGroupMember);
    exiledNode = null;
  }

  private DataGroupMember getDataGroupMember(PartitionGroup group, Node node) {
    return new DataGroupMember(null, group, node, new TestPartitionedLogManager(null,
        partitionTable, group.getHeader(), TestSnapshot::new),
        metaGroupMember) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      TSStatus executeNonQuery(PhysicalPlan plan) {
        try {
          planExecutor.processNonQuery(plan);
          return StatusUtils.OK;
        } catch (QueryProcessException e) {
          TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
          status.setMessage(e.getMessage());
          return status;
        }
      }

      @Override
      TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
        return executeNonQuery(plan);
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return null;
      }

      @Override
      public void pullTimeSeriesSchema(PullSchemaRequest request,
          AsyncMethodCallback<PullSchemaResp> resultHandler) {
        mockedPullTimeSeriesSchema(request, resultHandler);
      }
    };
  }

  private void mockedPullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    new Thread(() -> {
      try {
        List<MeasurementSchema> schemas = new ArrayList<>();
        List<String> prefixPaths = request.getPrefixPaths();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        for (String prefixPath : prefixPaths) {
          if (!prefixPath.equals(TestUtils.getTestSeries(10, 0))) {
            MManager.getInstance().collectSeries(prefixPath, schemas);
            dataOutputStream.writeInt(schemas.size());
            for (MeasurementSchema schema : schemas) {
              schema.serializeTo(dataOutputStream);
            }
          } else {
            dataOutputStream.writeInt(1);
            TestUtils.getTestSchema(10, 0).serializeTo(dataOutputStream);
          }
        }
        PullSchemaResp resp = new PullSchemaResp();
        resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
        resultHandler.onComplete(resp);
      } catch (IOException e) {
        resultHandler.onError(e);
      }
    }).start();
  }


  protected MetaGroupMember getMetaGroupMember(Node node) throws QueryProcessException {
    return new MetaGroupMember(new Factory(), node) {

      @Override
      public DataClusterServer getDataClusterServer() {
        return mockDataClusterServer ? MetaGroupMemberTest.this.dataClusterServer
            : super.getDataClusterServer();
      }

      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new TestDataClient(node, dataGroupMemberMap);
      }

      @Override
      protected DataGroupMember getLocalDataMember(Node header, AsyncMethodCallback resultHandler,
          Object request) {
        return getDataGroupMember(header);
      }

      @Override
      public AsyncClient connectNode(Node node) {
        if (node.equals(thisNode)) {
          return null;
        }
        try {
          return new TestMetaClient(null, null, node, null) {
            @Override
            public void startElection(ElectionRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(dummyResponse.get());
                }
              }).start();
            }

            @Override
            public void sendHeartbeat(HeartbeatRequest request,
                AsyncMethodCallback<HeartbeatResponse> resultHandler) {
              new Thread(() -> {
                HeartbeatResponse response = new HeartbeatResponse();
                response.setTerm(Response.RESPONSE_AGREE);
                resultHandler.onComplete(response);
              }).start();
            }

            @Override
            public void appendEntry(AppendEntryRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(dummyResponse.get());
                }
              }).start();
            }

            @Override
            public void addNode(Node node, StartUpStatus startUpStatus,
                AsyncMethodCallback<AddNodeResponse> resultHandler) {
              new Thread(() -> {
                if (node.getNodeIdentifier() == 10) {
                  resultHandler.onComplete(new AddNodeResponse(
                      (int) Response.RESPONSE_IDENTIFIER_CONFLICT));
                } else {
                  partitionTable.addNode(node);
                  AddNodeResponse resp = new AddNodeResponse((int) dummyResponse.get());
                  resp.setPartitionTableBytes(partitionTable.serialize());
                  resultHandler.onComplete(resp);
                }
              }).start();
            }

            @Override
            public void executeNonQueryPlan(ExecutNonQueryReq request,
                AsyncMethodCallback<TSStatus> resultHandler) {
              new Thread(() -> {
                try {
                  PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
                  planExecutor.processNonQuery(plan);
                  resultHandler.onComplete(StatusUtils.OK);
                } catch (IOException | QueryProcessException e) {
                  resultHandler.onError(e);
                }
              }).start();
            }

            @Override
            public void pullTimeSeriesSchema(PullSchemaRequest request,
                AsyncMethodCallback<PullSchemaResp> resultHandler) {
              mockedPullTimeSeriesSchema(request, resultHandler);
            }

            @Override
            public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
              new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
            }

            @Override
            public void exile(AsyncMethodCallback<Void> resultHandler) {
              new Thread(() -> exiledNode = node).start();
            }

            @Override
            public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                metaGroupMember.applyRemoveNode(node);
                resultHandler.onComplete(Response.RESPONSE_AGREE);
              }).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }
    };
  }

  private void buildDataGroups(DataClusterServer dataClusterServer) throws TTransportException {
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();

    dataClusterServer.setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      DataGroupMember dataGroupMember = getDataGroupMember(partitionGroup, TestUtils.getNode(0));
      dataGroupMember.start();
      dataClusterServer.addDataGroupMember(dataGroupMember);
    }
  }

  @Test
  public void testClosePartition() throws QueryProcessException, StorageEngineException {
    // the operation is accepted
    dummyResponse.set(Response.RESPONSE_AGREE);
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[]{String.valueOf(i)});
      PlanExecutor planExecutor = new PlanExecutor();
      planExecutor.processNonQuery(insertPlan);
    }
    metaGroupMember.closePartition(TestUtils.getTestSg(0), true);

    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

    // the operation times out
    dummyResponse.set(Long.MIN_VALUE);
    int prevTimeout = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 1;
    try {
      for (int i = 20; i < 30; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new String[]{String.valueOf(i)});
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      metaGroupMember.closePartition(TestUtils.getTestSg(0), true);
      assertFalse(processor.getWorkSequenceTsFileProcessors().isEmpty());

      // indicating the leader is stale
      dummyResponse.set(100);
      metaGroupMember.closePartition(TestUtils.getTestSg(0), true);
      assertFalse(processor.getWorkSequenceTsFileProcessors().isEmpty());
    } finally {
      RaftServer.connectionTimeoutInMS = prevTimeout;
    }
  }

  @Test
  public void testAddNode() {
    Node newNode = TestUtils.getNode(10);
    metaGroupMember.onElectionWins();
    metaGroupMember.applyAddNode(newNode);
    assertTrue(partitionTable.getAllNodes().contains(newNode));
  }

  @Test
  public void testBuildCluster() throws TTransportException {
    metaGroupMember.start();
    try {
      metaGroupMember.buildCluster();
      long startTime = System.currentTimeMillis();
      long timeConsumption = 0;
      while (timeConsumption < 5000 && metaGroupMember.getCharacter() != LEADER) {
        timeConsumption = System.currentTimeMillis() - startTime;
      }
      if (timeConsumption >= 5000) {
        fail("The member takes too long to be the leader");
      }
      assertEquals(LEADER, metaGroupMember.getCharacter());
    } finally {
      metaGroupMember.stop();
    }
  }

  @Test
  public void testJoinCluster() throws TTransportException, QueryProcessException {
    MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
    newMember.start();
    try {
      assertTrue(newMember.joinCluster());
      newMember.setCharacter(ELECTOR);
      long startTime = System.currentTimeMillis();
      long timeConsumption = 0;
      while (timeConsumption < 5000 && newMember.getCharacter() != LEADER) {
        timeConsumption = System.currentTimeMillis() - startTime;
      }
      if (timeConsumption >= 5000) {
        fail("The member takes too long to be the leader");
      }
      assertEquals(LEADER, newMember.getCharacter());
    } finally {
      newMember.stop();
    }
  }

  @Test
  public void testJoinClusterFailed() throws QueryProcessException {
    long prevInterval = RaftServer.heartBeatIntervalMs;
    RaftServer.heartBeatIntervalMs = 10;
    try {
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
      assertFalse(newMember.joinCluster());
    } finally {
      RaftServer.heartBeatIntervalMs = prevInterval;
    }
  }

  @Test
  public void testSendSnapshot() throws InterruptedException {
    SendSnapshotRequest request = new SendSnapshotRequest();
    List<String> newSgs = Collections.singletonList(TestUtils.getTestSg(10));
    List<Log> logs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      PhysicalPlanLog log = new PhysicalPlanLog();
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan();
      MeasurementSchema schema = TestUtils.getTestSchema(10, i);
      createTimeSeriesPlan.setPath(new Path(schema.getMeasurementId()));
      createTimeSeriesPlan.setDataType(schema.getType());
      createTimeSeriesPlan.setEncoding(schema.getEncodingType());
      createTimeSeriesPlan.setCompressor(schema.getCompressor());
      createTimeSeriesPlan.setProps(schema.getProps());
      log.setPlan(createTimeSeriesPlan);
      logs.add(log);
    }
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot(logs, newSgs);
    request.setSnapshotBytes(snapshot.serialize());
    AtomicReference<Void> reference = new AtomicReference<>();
    synchronized (reference) {
      metaGroupMember.sendSnapshot(request, new GenericHandler(TestUtils.getNode(0), reference));
      reference.wait(500);
    }

    for (int i = 0; i < 10; i++) {
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSeries(10, i)));
    }
  }

  @Test
  public void testProcessNonQuery() {
    mockDataClusterServer = true;
    // as a leader
    metaGroupMember.setCharacter(LEADER);
    for (int i = 10; i < 20; i++) {
      // process a non partitioned plan
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new Path(TestUtils.getTestSg(i)));
      TSStatus status = metaGroupMember.executeNonQuery(setStorageGroupPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSg(i)));

      // process a partitioned plan
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(
          new Path(schema.getMeasurementId()), schema.getType(),
          schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      status = metaGroupMember.executeNonQuery(createTimeSeriesPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSeries(i, 0)));
    }
  }

  @Test
  public void testPullTimeseriesSchema() throws MetadataException {
    for (int i = 1; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        MeasurementSchema schema = TestUtils.getTestSchema(i, j);
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType(),
            schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      }
    }

    for (int i = 0; i < 10; i++) {
      List<MeasurementSchema> schemas =
          metaGroupMember.pullTimeSeriesSchemas(Collections.singletonList(TestUtils.getTestSg(i)));
      assertEquals(20, schemas.size());
      for (int j = 0; j < 10; j++) {
        assertEquals(TestUtils.getTestSchema(i, j), schemas.get(j));
      }
    }
  }

  @Test
  public void testRemotePullTimeseriesSchema() throws MetadataException, InterruptedException {
    mockDataClusterServer = true;
    for (int i = 1; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        MeasurementSchema schema = TestUtils.getTestSchema(i, j);
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType(),
            schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      }
    }

    PullSchemaRequest request = new PullSchemaRequest();
    request.setHeader(TestUtils.getNode(0));
    for (int i = 0; i < 10; i++) {
      request.setPrefixPaths(Collections.singletonList(TestUtils.getTestSg(i)));
      AtomicReference<PullSchemaResp> result = new AtomicReference<>();
      GenericHandler<PullSchemaResp> handler = new GenericHandler<>(TestUtils.getNode(0)
          , result);
      synchronized (result) {
        metaGroupMember.pullTimeSeriesSchema(request, handler);
        result.wait(500);
      }
      PullSchemaResp resp = result.get();
      ByteBuffer schemaBuffer = resp.schemaBytes;
      List<MeasurementSchema> schemas = new ArrayList<>();
      int size = schemaBuffer.getInt();
      for (int j = 0; j < size; j++) {
        schemas.add(MeasurementSchema.deserializeFrom(schemaBuffer));
      }

      assertEquals(20, schemas.size());
      for (int j = 0; j < 10; j++) {
        assertEquals(TestUtils.getTestSchema(i, j), schemas.get(j));
      }
    }
  }

  @Test
  public void testGetSeriesType() throws MetadataException {
    // a local series
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        metaGroupMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(0, 0)),
                null));
    // a remote series that can be fetched
    MManager.getInstance().setStorageGroup(TestUtils.getTestSg(10));
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        metaGroupMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(10, 0)),
                null));
    // a non-existent series
    try {
      metaGroupMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(10
          , 100)), null);
    } catch (PathNotExistException e) {
      assertEquals("Path [root.test10.s100] does not exist", e.getMessage());
    }
    // a non-existent group
    try {
      metaGroupMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(11
          , 100)), null);
    } catch (StorageGroupNotSetException e) {
      assertEquals("Storage group is not set for current seriesPath: [root.test11.s100]",
          e.getMessage());
    }
  }

  @Test
  public void testGetReaderByTimestamp()
      throws QueryProcessException, StorageEngineException, IOException {
    mockDataClusterServer = true;
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[]{String.valueOf(j)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        IReaderByTimestamp readerByTimestamp = metaGroupMember
            .getReaderByTimestamp(new Path(TestUtils.getTestSeries(i, 0)), TSDataType.DOUBLE,
                context);
        for (int j = 0; j < 10; j++) {
          assertEquals(j * 1.0, (double) readerByTimestamp.getValueInTimestamp(j), 0.00001);
        }
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetReader() throws QueryProcessException, StorageEngineException, IOException {
    mockDataClusterServer = true;
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDataTypes(new TSDataType[]{TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[]{String.valueOf(j)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        ManagedSeriesReader reader = metaGroupMember
            .getSeriesReader(new Path(TestUtils.getTestSeries(i, 0)), TSDataType.DOUBLE,
                TimeFilter.gtEq(5),
                ValueFilter.ltEq(8.0), context);
        assertTrue(reader.hasNextBatch());
        BatchData batchData = reader.nextBatch();
        for (int j = 5; j < 9; j++) {
          assertTrue(batchData.hasCurrent());
          assertEquals(j, batchData.currentTime());
          assertEquals(j * 1.0, batchData.getDouble(), 0.00001);
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
        assertFalse(reader.hasNextBatch());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetMatchedPaths() throws MetadataException {
    List<String> matchedPaths = metaGroupMember
        .getMatchedPaths(TestUtils.getTestSg(0) + ".*");
    assertEquals(20, matchedPaths.size());
    for (int j = 0; j < 10; j++) {
      assertEquals(TestUtils.getTestSeries(0, j), matchedPaths.get(j));
    }
    matchedPaths = metaGroupMember
        .getMatchedPaths(TestUtils.getTestSg(10) + ".*");
    assertTrue(matchedPaths.isEmpty());
  }

  @Test
  public void testProcessValidHeartbeatReq() throws QueryProcessException {
    MetaGroupMember metaGroupMember = getMetaGroupMember(TestUtils.getNode(10));
    try {
      HeartbeatRequest request = new HeartbeatRequest();
      request.setRequireIdentifier(true);
      HeartbeatResponse response = new HeartbeatResponse();
      metaGroupMember.processValidHeartbeatReq(request, response);
      assertEquals(10, response.getFollowerIdentifier());

      request.setRegenerateIdentifier(true);
      metaGroupMember.processValidHeartbeatReq(request, response);
      assertNotEquals(10, response.getFollowerIdentifier());
      assertTrue(response.isRequirePartitionTable());

      request.setPartitionTableBytes(partitionTable.serialize());
      metaGroupMember.processValidHeartbeatReq(request, response);
      assertEquals(partitionTable, metaGroupMember.getPartitionTable());
    } finally {
      metaGroupMember.stop();
    }
  }

  @Test
  public void testProcessValidHeartbeatResp()
      throws TTransportException, QueryProcessException {
    MetaGroupMember metaGroupMember = getMetaGroupMember(TestUtils.getNode(10));
    metaGroupMember.start();
    metaGroupMember.onElectionWins();
    try {
      for (int i = 0; i < 10; i++) {
        HeartbeatResponse response = new HeartbeatResponse();
        response.setFollowerIdentifier(i);
        response.setRequirePartitionTable(true);
        metaGroupMember.processValidHeartbeatResp(response, TestUtils.getNode(i));
        metaGroupMember.removeBlindNode(TestUtils.getNode(i));
      }
      assertNotNull(metaGroupMember.getPartitionTable());
    } finally {
      metaGroupMember.stop();
    }
  }

  @Test
  public void testAppendEntry() throws InterruptedException {
    metaGroupMember.setPartitionTable(null);
    CloseFileLog log = new CloseFileLog(TestUtils.getTestSg(0), true);
    log.setCurrLogIndex(0);
    log.setCurrLogTerm(0);
    log.setPreviousLogIndex(-1);
    log.setPreviousLogTerm(-1);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setEntry(log.serialize());
    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      metaGroupMember.appendEntry(request, handler);
      result.wait(500);
    }
    assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, (long) result.get());
    assertNull(metaGroupMember.getLogManager().getLastLog());

    metaGroupMember.setPartitionTable(partitionTable);
    synchronized (result) {
      metaGroupMember.appendEntry(request, handler);
      result.wait(500);
    }
    assertEquals(Response.RESPONSE_AGREE, (long) result.get());
    assertEquals(log, metaGroupMember.getLogManager().getLastLog());
  }

  @Test
  public void testRemoteAddNode() throws InterruptedException, TTransportException {
    metaGroupMember.start();
    int prevTimeout = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 100;
    try {
      // cannot add node when partition table is not built
      metaGroupMember.setPartitionTable(null);
      AtomicReference<AddNodeResponse> result = new AtomicReference<>();
      GenericHandler<AddNodeResponse> handler = new GenericHandler<>(TestUtils.getNode(0), result);
      synchronized (result) {

        metaGroupMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      AddNodeResponse response = result.get();
      assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, response.getRespNum());

      // cannot add itself
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(0), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      assertNull(result.get());

      // process the request as a leader
      metaGroupMember.setCharacter(LEADER);
      metaGroupMember.onElectionWins();
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // adding an existing node is ok
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // process the request as a follower
      metaGroupMember.setCharacter(FOLLOWER);
      metaGroupMember.setLeader(TestUtils.getNode(1));
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(11), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // cannot add a node with conflict id
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        Node node = TestUtils.getNode(12).setNodeIdentifier(10);
        metaGroupMember.addNode(node, TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_IDENTIFIER_CONFLICT, response.getRespNum());

      // cannot add a node due to network failure, the request should forwarded
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(12), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertNull(response);

      // cannot add a node due to leadership lost
      dummyResponse.set(100);
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        metaGroupMember.addNode(TestUtils.getNode(12), TestUtils.getStartUpStatus(), handler);
        result.wait(200);
      }
      response = result.get();
      assertNull(response);

      // cannot add a node due to configuration conflict, partition interval
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        Node node = TestUtils.getNode(12);
        StartUpStatus startUpStatus = TestUtils.getStartUpStatus();
        startUpStatus.setPartitionInterval(0);
        metaGroupMember.addNode(node, startUpStatus, handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertFalse(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, hash salt
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        Node node = TestUtils.getNode(12);
        StartUpStatus startUpStatus = TestUtils.getStartUpStatus();
        startUpStatus.setHashSalt(0);
        metaGroupMember.addNode(node, startUpStatus, handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertFalse(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, replication number
      metaGroupMember.setCharacter(LEADER);
      result.set(null);
      metaGroupMember.setPartitionTable(partitionTable);
      synchronized (result) {
        Node node = TestUtils.getNode(12);
        StartUpStatus startUpStatus = TestUtils.getStartUpStatus();
        startUpStatus.setReplicationNumber(0);
        metaGroupMember.addNode(node, startUpStatus, handler);
        result.wait(200);
      }
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertFalse(response.getCheckStatusResponse().isReplicationNumEquals());


    } finally {
      metaGroupMember.stop();
      RaftServer.connectionTimeoutInMS = prevTimeout;
    }
  }

  @Test
  public void testLoadIdentifier() throws IOException, QueryProcessException {
    try (RandomAccessFile raf = new RandomAccessFile(MetaGroupMember.NODE_IDENTIFIER_FILE_NAME,
        "rw")) {
      raf.writeBytes("100");
    }
    MetaGroupMember metaGroupMember = getMetaGroupMember(new Node());
    assertEquals(100, metaGroupMember.getThisNode().getNodeIdentifier());
  }

  @Test
  public void testRemoveNodeWithoutPartitionTable() throws InterruptedException {
    metaGroupMember.setPartitionTable(null);
    AtomicBoolean passed = new AtomicBoolean(false);
    synchronized (metaGroupMember) {
      metaGroupMember.removeNode(TestUtils.getNode(0), new AsyncMethodCallback<Long>() {
        @Override
        public void onComplete(Long aLong) {
          synchronized (metaGroupMember) {
            metaGroupMember.notifyAll();
          }
        }

        @Override
        public void onError(Exception e) {
          new Thread(() -> {
            synchronized (metaGroupMember) {
              passed.set(e instanceof PartitionTableUnavailableException);
              metaGroupMember.notifyAll();
            }
          }).start();
        }
      });
      metaGroupMember.wait(500);
    }

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveThisNode() throws InterruptedException {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    metaGroupMember.setLeader(metaGroupMember.getThisNode());
    metaGroupMember.setCharacter(LEADER);
    doRemoveNode(resultRef, metaGroupMember.getThisNode());
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(metaGroupMember.getAllNodes().contains(metaGroupMember.getThisNode()));
  }

  @Test
  public void testRemoveLeader() throws InterruptedException {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    metaGroupMember.setLeader(TestUtils.getNode(40));
    metaGroupMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(40));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(metaGroupMember.getAllNodes().contains(TestUtils.getNode(40)));
    assertEquals(ELECTOR, metaGroupMember.getCharacter());
    assertEquals(Long.MIN_VALUE, metaGroupMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNonLeader() throws InterruptedException {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    metaGroupMember.setLeader(TestUtils.getNode(40));
    metaGroupMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(metaGroupMember.getAllNodes().contains(TestUtils.getNode(20)));
    assertEquals(0, metaGroupMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNodeAsLeader() throws InterruptedException {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    metaGroupMember.setLeader(metaGroupMember.getThisNode());
    metaGroupMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(metaGroupMember.getAllNodes().contains(TestUtils.getNode(20)));
    assertEquals(TestUtils.getNode(20), exiledNode);
  }

  @Test
  public void testRemoveNonExistNode() throws InterruptedException {
    AtomicBoolean passed = new AtomicBoolean(false);
    metaGroupMember.setCharacter(LEADER);
    metaGroupMember.setLeader(metaGroupMember.getThisNode());
    synchronized (metaGroupMember) {
      metaGroupMember.removeNode(TestUtils.getNode(120), new AsyncMethodCallback<Long>() {
        @Override
        public void onComplete(Long aLong) {
          synchronized (metaGroupMember) {
            passed.set(aLong.equals(Response.RESPONSE_REJECT));
            metaGroupMember.notifyAll();
          }
        }

        @Override
        public void onError(Exception e) {
          new Thread(() -> {
            synchronized (metaGroupMember) {
              e.printStackTrace();
              metaGroupMember.notifyAll();
            }
          }).start();
        }
      });
      metaGroupMember.wait(500);
    }

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveTooManyNodes() throws InterruptedException {
    for (int i = 0; i < 7; i++) {
      AtomicReference<Long> resultRef = new AtomicReference<>();
      metaGroupMember.setCharacter(LEADER);
      doRemoveNode(resultRef, TestUtils.getNode(90 - i * 10));
      assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
      assertFalse(metaGroupMember.getAllNodes().contains(TestUtils.getNode(90 - i * 10)));
    }
    AtomicReference<Long> resultRef = new AtomicReference<>();
    metaGroupMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_CLUSTER_TOO_SMALL, (long) resultRef.get());
    assertTrue(metaGroupMember.getAllNodes().contains(TestUtils.getNode(20)));
  }

  private void doRemoveNode(AtomicReference<Long> resultRef, Node nodeToRemove)
      throws InterruptedException {
    synchronized (resultRef) {
      metaGroupMember.removeNode(nodeToRemove, new AsyncMethodCallback<Long>() {
        @Override
        public void onComplete(Long o) {
          new Thread(() -> {
            synchronized (resultRef) {
              resultRef.set(o);
              resultRef.notifyAll();
            }
          }).start();
        }

        @Override
        public void onError(Exception e) {
          new Thread(() -> {
            synchronized (resultRef) {
              e.printStackTrace();
              resultRef.notifyAll();
            }
          }).start();
        }
      });
      resultRef.wait(500);
    }
  }
}