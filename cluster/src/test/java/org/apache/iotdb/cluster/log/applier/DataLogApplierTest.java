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

package org.apache.iotdb.cluster.log.applier;

import junit.framework.TestCase;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.common.*;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.service.DataAsyncService;
import org.apache.iotdb.cluster.server.service.MetaAsyncService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class DataLogApplierTest extends IoTDBTest {

  private boolean partialWriteEnabled;

  private TestMetaGroupMember testMetaGroupMember = new TestMetaGroupMember() {
    @Override
    public boolean syncLeader() {
      return true;
    }

    @Override
    public DataGroupMember getLocalDataMember(Node header,
                                              AsyncMethodCallback resultHandler, Object request) {
      return testDataGroupMember;
    }

    @Override
    public List<MeasurementSchema> pullTimeSeriesSchemas(List<String> prefixPaths)
      throws StorageGroupNotSetException {
      List<MeasurementSchema> ret = new ArrayList<>();
      for (String prefixPath : prefixPaths) {
        if (prefixPath.startsWith(TestUtils.getTestSg(4))) {
          for (int i = 0; i < 10; i++) {
            ret.add(TestUtils.getTestMeasurementSchema(i));
          }
        } else if (!prefixPath.startsWith(TestUtils.getTestSg(5))) {
          throw new StorageGroupNotSetException(prefixPath);
        }
      }
      return ret;
    }

    @Override
    public AsyncClient getAsyncClient(Node node) {
      try {
        return new TestAsyncMetaClient(null, null, node, null) {
          @Override
          public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
            new Thread(() -> new MetaAsyncService(testMetaGroupMember).queryNodeStatus(resultHandler)).start();
          }
        };
      } catch (IOException e) {
        return null;
      }
    }

    @Override
    public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
      return new AsyncDataClient(null, null, node, null) {
        @Override
        public void getAllPaths(Node header, List<String> path,
            AsyncMethodCallback<List<String>> resultHandler) {
          new Thread(() -> new DataAsyncService(testDataGroupMember).getAllPaths(header, path, resultHandler)).start();
        }
      };
    }
  };

  private TestDataGroupMember testDataGroupMember = new TestDataGroupMember();

  private LogApplier applier = new DataLogApplier(testMetaGroupMember, testDataGroupMember);

  @Override
  @Before
  public void setUp() throws org.apache.iotdb.db.exception.StartupException, QueryProcessException {
    IoTDB.setMetaManager(CMManager.getInstance());
    MetaPuller.getInstance().init(testMetaGroupMember);
    super.setUp();
    MetaPuller.getInstance().init(testMetaGroupMember);
    PartitionGroup allNodes = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      allNodes.add(TestUtils.getNode(i));
    }

    testMetaGroupMember.setAllNodes(allNodes);
    testMetaGroupMember.setPartitionTable(new SlotPartitionTable(allNodes, TestUtils.getNode(0)));
    testMetaGroupMember.setThisNode(TestUtils.getNode(0));

    testMetaGroupMember.setLeader(testMetaGroupMember.getThisNode());
    testDataGroupMember.setLeader(testDataGroupMember.getThisNode());
    testDataGroupMember.setCharacter(NodeCharacter.LEADER);
    testMetaGroupMember.setCharacter(NodeCharacter.LEADER);
    QueryCoordinator.getINSTANCE().setMetaGroupMember(testMetaGroupMember);
    partialWriteEnabled = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(false);
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    testDataGroupMember.closeLogManager();
    testMetaGroupMember.closeLogManager();
    super.tearDown();
    QueryCoordinator.getINSTANCE().setMetaGroupMember(null);
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(partialWriteEnabled);
  }

  @Test
  public void testApplyInsert()
    throws QueryProcessException, IOException, QueryFilterOptimizationException, StorageEngineException, MetadataException, TException, InterruptedException {
    InsertRowPlan insertPlan = new InsertRowPlan();
    PhysicalPlanLog log = new PhysicalPlanLog();
    log.setPlan(insertPlan);

    // this series is already created
    insertPlan.setDeviceId(TestUtils.getTestSg(1));
    insertPlan.setTime(1);
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    insertPlan.setValues(new Object[]{"1.0"});
    insertPlan.setNeedInferType(true);
    insertPlan
      .setSchemasAndTransferType(new MeasurementSchema[]{TestUtils.getTestMeasurementSchema(0)});

    applier.apply(log);
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(1, 0)), null);
    assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series is not created but can be fetched
    insertPlan.setDeviceId(TestUtils.getTestSg(4));
    applier.apply(log);
    dataSet = query(Collections.singletonList(TestUtils.getTestSeries(4, 0)), null);
    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series does not exists any where
    insertPlan.setDeviceId(TestUtils.getTestSg(5));
    try {
      applier.apply(log);
      fail("exception should be thrown");
    } catch (QueryProcessException e) {
      assertEquals(
        "org.apache.iotdb.db.exception.metadata.PathNotExistException: Path [root.test5.s0] does not exist",
        e.getMessage());
    }

    // this storage group is not even set
    insertPlan.setDeviceId(TestUtils.getTestSg(6));
    try {
      applier.apply(log);
      fail("exception should be thrown");
    } catch (QueryProcessException e) {
      assertEquals(
        "org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException: Storage group is not set for current seriesPath: [root.test6.s0]",
        e.getMessage());
    }
  }

  @Test
  public void testApplyDeletion()
    throws QueryProcessException, MetadataException, QueryFilterOptimizationException, StorageEngineException, IOException, TException, InterruptedException {
    DeletePlan deletePlan = new DeletePlan();
    deletePlan.setPaths(Collections.singletonList(new Path(TestUtils.getTestSeries(0, 0))));
    deletePlan.setDeleteEndTime(50);
    applier.apply(new PhysicalPlanLog(deletePlan));
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(0, 0)), null);
    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(cnt + 51L, record.getTimestamp());
      assertEquals((cnt + 51) * 1.0, record.getFields().get(0).getDoubleV(), 0.00001);
      cnt++;
    }
    assertEquals(49, cnt);
  }

  @Test
  public void testApplyCloseFile()
    throws StorageEngineException, QueryProcessException, StorageGroupNotSetException {
    StorageGroupProcessor storageGroupProcessor =
      StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    TestCase.assertFalse(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());

    CloseFileLog closeFileLog = new CloseFileLog(TestUtils.getTestSg(0), 0, true);
    applier.apply(closeFileLog);
    TestCase.assertTrue(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());
  }
}