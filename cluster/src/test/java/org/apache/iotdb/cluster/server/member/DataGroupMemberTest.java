/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataGroupMemberTest extends MemberTest {

  private PartitionGroup partitionGroup;
  private DataGroupMember dataGroupMember;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    partitionGroup = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      partitionGroup.add(TestUtils.getNode(i));
    }

    dataGroupMember = getDataGroupMember(TestUtils.getNode(0));
  }

  private DataGroupMember getDataGroupMember(Node node) throws IOException {
    return new DataGroupMember(new TCompactProtocol.Factory(), new PartitionGroup(partitionGroup)
        , node, new TestPartitionedLogManager(new DataLogApplier(metaGroupMember),
            metaGroupMember.getPartitionTable(), partitionGroup.getHeader(), TestSnapshot::new),
        metaGroupMember, new TAsyncClientManager()) {
      @Override
      public AsyncClient connectNode(Node node) {
        return new TestClient() {
          @Override
          public void readFile(String filePath, long offset, int length, Node header,
              AsyncMethodCallback<ByteBuffer> resultHandler) {
            resultHandler.onComplete(ByteBuffer.allocate(0));
          }

          @Override
          public void startElection(ElectionRequest request,
              AsyncMethodCallback<Long> resultHandler) {
          }
        };
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
    metaGroupMember.getTerm().set(10);
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
    assertEquals(Response.RESPONSE_META_LOG_STALE, dataGroupMember.processElectionRequest(electionRequest));

    // a request with stale data log
    electionRequest.setTerm(12);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(1);
    electionRequest.setDataLogLastIndex(1);
    assertEquals(Response.RESPONSE_LOG_MISMATCH, dataGroupMember.processElectionRequest(electionRequest));
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
  public void testApplySnapshot() throws StorageEngineException, QueryProcessException {
    FileSnapshot snapshot = new FileSnapshot();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      schemaList.add(TestUtils.getTestSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(schemaList);

    // resource1 exists locally
    RemoteTsFileResource resource1 = new RemoteTsFileResource();
    resource1.setFile(new File(TestUtils.getTestSg(0), "0-" + 0 + "-0.tsfile"));
    resource1.setHistoricalVersions(Collections.singleton(0L));
    // resource2 does not exist locally but without modification
    RemoteTsFileResource resource2 = new RemoteTsFileResource();
    resource2.setFile(new File(TestUtils.getTestSg(0), "0-" + 1 + "-0.tsfile"));
    resource1.setHistoricalVersions(Collections.singleton(1L));
    // resource3 exists locally and with modification
    RemoteTsFileResource resource3 = new RemoteTsFileResource();
    resource3.setFile(new File(TestUtils.getTestSg(0), "0-" + 2 + "-0.tsfile"));
    resource1.setHistoricalVersions(Collections.singleton(2L));
    resource3.setWithModification(true);
    snapshot.addFile(resource1, TestUtils.getNode(0));
    snapshot.addFile(resource2, TestUtils.getNode(0));
    snapshot.addFile(resource3, TestUtils.getNode(0));

    // create a local resource1
    StorageGroupProcessor processor = StorageEngine.getInstance()
        .getProcessor(TestUtils.getTestSg(0));
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setTime(0);
    insertPlan.setMeasurements(new String[] {"s0"});
    insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
    insertPlan.setValues(new String[] {"1.0"});
    processor.insert(insertPlan);
    processor.waitForAllCurrentTsFileProcessorsClosed();

    dataGroupMember.applySnapshot(snapshot, 0);
    assertEquals(3, processor.getSequenceFileList().size());
  }
}