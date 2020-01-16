/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import static org.apache.iotdb.cluster.server.NodeCharacter.ELECTOR;
import static org.apache.iotdb.cluster.server.NodeCharacter.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
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
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithValueFilter;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetaGroupMemberTest extends MemberTest{

  private MetaGroupMember metaGroupMember;
  private DataGroupMember dataGroupMember;
  private DataClusterServer dataClusterServer;
  private AtomicLong response;
  private boolean mockDataClusterServer;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    response = new AtomicLong(Response.RESPONSE_AGREE);
    metaGroupMember = getMetaGroupMember(TestUtils.getNode(0));
    // a faked data member to respond requests
    dataGroupMember = getDataGroupMember(partitionGroup, TestUtils.getNode(0));
    dataGroupMember.setCharacter(LEADER);
    dataClusterServer = new DataClusterServer(TestUtils.getNode(0),
        new DataGroupMember.Factory(null, metaGroupMember, null, null) {
          @Override
          public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
            return getDataGroupMember(partitionGroup, thisNode);
          }
        }){
    };
    buildDataGroups(dataClusterServer);
    metaGroupMember.setPartitionTable(partitionTable);
    metaGroupMember.getThisNode().setNodeIdentifier(0);
    mockDataClusterServer = false;
    QueryCoordinator.getINSTANCE().setMetaGroupMember(metaGroupMember);
  }

  private DataGroupMember getDataGroupMember(PartitionGroup group, Node node) {
    return new DataGroupMember(null, group, node, new TestPartitionedLogManager(null,
        partitionTable, partitionGroup.getHeader(), TestSnapshot::new),
        metaGroupMember, null) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      TSStatus executeNonQuery(PhysicalPlan plan) {
        try {
          queryProcessExecutor.processNonQuery(plan);
          return StatusUtils.OK;
        } catch (QueryProcessException e) {
          TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
          status.getStatusType().setMessage(e.getMessage());
          return status;
        }
      }

      @Override
      TSStatus forwardPlan(PhysicalPlan plan, Node node) {
        return executeNonQuery(plan);
      }

      @Override
      TSStatus forwardPlan(PhysicalPlan plan, PartitionGroup group) {
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

      @Override
      IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context)
          throws StorageEngineException, IOException {
        return new SeriesReaderByTimestamp(path, context);
      }

      @Override
      ManagedSeriesReader getSeriesReaderWithoutValueFilter(Path path, TSDataType dataType,
          Filter timeFilter, QueryContext context, boolean pushdownUnseq)
          throws IOException, StorageEngineException {
        return new SeriesReaderWithoutValueFilter(path, dataType, timeFilter, context, pushdownUnseq);
      }

      @Override
      ManagedSeriesReader getSeriesReaderWithValueFilter(Path path, TSDataType dataType,
          Filter timeFilter, QueryContext context) throws IOException, StorageEngineException {
        return new SeriesReaderWithValueFilter(path, dataType, timeFilter, context);
      }
    };
  }

  private void mockedPullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    new Thread(() -> {
      try {
        List<MeasurementSchema> schemas = new ArrayList<>();
        String prefixPath = request.getPrefixPath();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        if (!prefixPath.equals(TestUtils.getTestSeries(10, 0))) {
          MManager.getInstance().collectSeries(prefixPath, schemas);
          dataOutputStream.writeInt(schemas.size());
          for (MeasurementSchema schema : schemas) {
            schema.serializeTo(dataOutputStream);
          }
        } else {
          dataOutputStream.writeInt(10);
          for (int i = 0; i < 10; i++) {
            TestUtils.getTestSchema(10, i).serializeTo(dataOutputStream);
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


  private MetaGroupMember getMetaGroupMember(Node node) throws IOException {
    return new MetaGroupMember(new Factory(), node) {

      @Override
      public DataClusterServer getDataClusterServer() {
        return mockDataClusterServer ? MetaGroupMemberTest.this.dataClusterServer : super.getDataClusterServer();
      }

      @Override
      public ClientPool getDataClientPool() {
        return new ClientPool(null) {
          @Override
          public AsyncClient getClient(Node node) throws IOException {
            return new TestDataClient(node) {
              private AtomicLong readerId = new AtomicLong();
              private Map<Long, IReaderByTimestamp> readerByTimestampMap = new HashMap<>();
              @Override
              public void querySingleSeries(SingleSeriesQueryRequest request,
                  AsyncMethodCallback<Long> resultHandler) {
                new Thread(() -> dataGroupMember.querySingleSeries(request, resultHandler)).start();
              }

              @Override
              public void fetchSingleSeries(Node header, long readerId,
                  AsyncMethodCallback<ByteBuffer> resultHandler) {
                new Thread(() -> dataGroupMember.fetchSingleSeries(header, readerId, resultHandler)).start();
              }

              @Override
              public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
                  AsyncMethodCallback<Long> resultHandler) {
                new Thread(() -> dataGroupMember.querySingleSeriesByTimestamp(request, resultHandler)).start();
              }

              @Override
              public void fetchSingleSeriesByTimestamp(Node header, long readerId, long timestamp,
                  AsyncMethodCallback<ByteBuffer> resultHandler) {
                new Thread(() -> dataGroupMember.fetchSingleSeriesByTimestamp(header, readerId, timestamp,
                    resultHandler)).start();
              }

              @Override
              public void getAllPaths(Node header, String path,
                  AsyncMethodCallback<List<String>> resultHandler) {
                new Thread(() -> {
                  try {
                    resultHandler.onComplete(MManager.getInstance().getPaths(path));
                  } catch (MetadataException e) {
                    resultHandler.onError(e);
                  }
                }).start();
              }
            };
          }
        };
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestMetaClient(null, null, node, null) {
            @Override
            public void startElection(ElectionRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = response.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(response.get());
                }
              }).start();
            }

            @Override
            public void sendHeartBeat(HeartBeatRequest request,
                AsyncMethodCallback<HeartBeatResponse> resultHandler) {
              new Thread(() -> {
                HeartBeatResponse response = new HeartBeatResponse();
                response.setTerm(Response.RESPONSE_AGREE);
                resultHandler.onComplete(response);
              }).start();
            }

            @Override
            public void appendEntry(AppendEntryRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = response.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(response.get());
                }
              }).start();
            }

            @Override
            public void addNode(Node node, AsyncMethodCallback<AddNodeResponse> resultHandler) {
              new Thread(() -> {
                partitionTable.addNode(node);
                AddNodeResponse resp = new AddNodeResponse((int) response.get());
                resp.setPartitionTableBytes(partitionTable.serialize());
                resultHandler.onComplete(resp);
              }).start();
            }

            @Override
            public void executeNonQueryPlan(ExecutNonQueryReq request,
                AsyncMethodCallback<TSStatus> resultHandler) {
              new Thread(() -> {
                try {
                  PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
                  queryProcessExecutor.processNonQuery(plan);
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

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testClosePartition() throws QueryProcessException, StorageEngineException {
    // the operation is accepted
    response.set(Response.RESPONSE_AGREE);
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
    metaGroupMember.closePartition(TestUtils.getTestSg(0), true);

    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

    // the operation times out
    response.set(Long.MIN_VALUE);
    int prevTimeout = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 1;
    try {
      for (int i = 20; i < 30; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new String[] {String.valueOf(i)});
        QueryProcessExecutor queryProcessExecutor = new QueryProcessExecutor();
        queryProcessExecutor.processNonQuery(insertPlan);
      }
      metaGroupMember.closePartition(TestUtils.getTestSg(0), true);
      assertFalse(processor.getWorkSequenceTsFileProcessors().isEmpty());

      // indicating the leader is stale
      response.set(100);
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
  public void testJoinCluster() throws TTransportException, IOException {
    MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
    newMember.start();
    try {
      newMember.joinCluster();
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
      assertTrue(MManager.getInstance().pathExist(TestUtils.getTestSeries(10, i)));
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
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.statusType.code);
      assertTrue(MManager.getInstance().pathExist(TestUtils.getTestSg(i)));

      // process a partitioned plan
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(new Path(schema.getMeasurementId()), schema.getType(),
          schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      status = metaGroupMember.executeNonQuery(createTimeSeriesPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.statusType.code);
      assertTrue(MManager.getInstance().pathExist(TestUtils.getTestSeries(i, 0)));
    }
  }

  @Test
  public void testPullTimeseriesSchema() throws MetadataException {
    for (int i = 1; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        MeasurementSchema schema = TestUtils.getTestSchema(i, j);
        MManager.getInstance().addPathToMTree(schema.getMeasurementId(), schema.getType(),
            schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      }
    }

    for (int i = 0; i < 10; i++) {
      List<MeasurementSchema> schemas =
          metaGroupMember.pullTimeSeriesSchemas(TestUtils.getTestSg(i));
      assertEquals(10, schemas.size());
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
        MManager.getInstance().addPathToMTree(schema.getMeasurementId(), schema.getType(),
            schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      }
    }

    PullSchemaRequest request = new PullSchemaRequest();
    request.setHeader(TestUtils.getNode(0));
    for (int i = 0; i < 10; i++) {
      request.setPrefixPath(TestUtils.getTestSg(i));
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

      assertEquals(10, schemas.size());
      for (int j = 0; j < 10; j++) {
        assertEquals(TestUtils.getTestSchema(i, j), schemas.get(j));
      }
    }
  }

  @Test
  public void testGetSeriesType() throws MetadataException {
    // a local series
    assertEquals(TSDataType.DOUBLE, metaGroupMember.getSeriesType(TestUtils.getTestSeries(0, 0)));
    // a remote series that can be fetched
    MManager.getInstance().setStorageGroupToMTree(TestUtils.getTestSg(10));
    assertEquals(TSDataType.DOUBLE, metaGroupMember.getSeriesType(TestUtils.getTestSeries(10, 0)));
    // a non-existent series
    try {
      metaGroupMember.getSeriesType(TestUtils.getTestSeries(10, 100));
    } catch (PathNotExistException e) {
      assertEquals("Path [root.test10.s100] does not exist", e.getMessage());
    }
    // a non-existent group
    try {
      metaGroupMember.getSeriesType(TestUtils.getTestSeries(11, 100));
    } catch (StorageGroupNotSetException e) {
      assertEquals("Storage group is not set for current seriesPath: [root.test11.s100]", e.getMessage());
    }
  }

  @Test
  public void testGetReaderByTimestamp()
      throws QueryProcessException, StorageEngineException, IOException {
    mockDataClusterServer = true;
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().addPathToMTree(new Path(schema.getMeasurementId()), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[] {String.valueOf(j)});
        queryProcessExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        IReaderByTimestamp readerByTimestamp = metaGroupMember
            .getReaderByTimestamp(new Path(TestUtils.getTestSeries(i, 0)), context);
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
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().addPathToMTree(new Path(schema.getMeasurementId()), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[] {String.valueOf(j)});
        queryProcessExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      Filter filter = new AndFilter(TimeFilter.gtEq(5), ValueFilter.ltEq(8.0));
      for (int i = 0; i < 10; i++) {
        ManagedSeriesReader reader = metaGroupMember
            .getSeriesReader(new Path(TestUtils.getTestSeries(i, 0)), TSDataType.DOUBLE, filter,
                context, true, true);
        for (int j = 5; j < 9; j++) {
          assertTrue(reader.hasNext());
          TimeValuePair pair = reader.next();
          assertEquals(j, pair.getTimestamp());
          assertEquals(j * 1.0, pair.getValue().getDouble(), 0.00001);
        }
        assertFalse(reader.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetMatchedPaths() throws MetadataException {
    List<String> matchedPaths = metaGroupMember
        .getMatchedPaths(TestUtils.getTestSg(0), TestUtils.getTestSg(0));
    assertEquals(10, matchedPaths.size());
    for (int j = 0; j < 10; j++) {
      assertEquals(TestUtils.getTestSeries(0, j), matchedPaths.get(j));
    }
    matchedPaths = metaGroupMember
        .getMatchedPaths(TestUtils.getTestSg(10), TestUtils.getTestSg(10));
    assertTrue(matchedPaths.isEmpty());
  }
}