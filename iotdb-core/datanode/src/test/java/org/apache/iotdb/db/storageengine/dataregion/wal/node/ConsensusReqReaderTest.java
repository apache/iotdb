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
package org.apache.iotdb.db.storageengine.dataregion.wal.node;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConsensusReqReaderTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String devicePath = "root.test_sg.test_d";
  private WALMode prevMode;
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    config.setWalMode(prevMode);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  /**
   * Generate wal files as below: <br>
   * _0-0-1.wal: 1,-1 <br>
   * _1-1-1.wal: 2,2,2 <br>
   * _2-2-1.wal: 3,3 <br>
   * _3-3-1.wal: 3,4 <br>
   * _4-4-1.wal: 4 <br>
   * _5-4-1.wal: 4,4,5 <br>
   * _6-5-1.wal: 6 <br>
   * 1 - InsertRowNode, 2 - InsertRowsOfOneDeviceNode, 3 - InsertRowsNode, 4 -
   * InsertMultiTabletsNode, 5 - InsertTabletNode, 6 - InsertRowNode
   */
  private void simulateFileScenario01() throws IllegalPathException {
    InsertTabletNode insertTabletNode;
    InsertRowNode insertRowNode;
    InsertRowsNode insertRowsNode;
    // _0-0-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(1);
    walNode.log(0, insertRowNode); // 1
    insertTabletNode = getInsertTabletNode(devicePath, new long[] {2});
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // -1
    walNode.rollWALFile();
    // _1-1-1.wal
    insertRowsNode = getInsertRowsNode(devicePath);
    insertRowsNode.setSearchIndex(2);
    walNode.log(0, insertRowsNode); // 2
    walNode.log(0, insertRowsNode); // 2
    walNode.log(0, insertRowsNode); // 2
    walNode.rollWALFile();
    // _2-2-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(3);
    walNode.log(0, insertRowNode); // 3
    walNode.log(0, insertRowNode); // 3
    walNode.rollWALFile();
    // _3-3-1.wal
    insertRowNode.setTargetPath(new PartialPath(devicePath + "test"));
    walNode.log(0, insertRowNode); // 3
    insertTabletNode = getInsertTabletNode(devicePath, new long[] {4});
    insertTabletNode.setSearchIndex(4);
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // 4
    walNode.rollWALFile();
    // _4-4-1.wal
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // 4
    walNode.rollWALFile();
    // _5-4-1.wal
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // 4
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // 4
    insertTabletNode = getInsertTabletNode(devicePath, new long[] {5});
    insertTabletNode.setSearchIndex(5);
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // 5
    walNode.rollWALFile();
    // _6-5-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(6);
    walNode.log(0, insertRowNode); // 6
  }

  @Test
  public void scenario01TestGetReqIterator01() throws Exception {
    simulateFileScenario01();
    walNode.rollWALFile();

    IndexedConsensusRequest request;
    PlanNode planNode;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowNode);
      Assert.assertEquals(1, ((InsertRowNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowsNode);
      Assert.assertEquals(2, ((InsertRowsNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowNode);
      Assert.assertEquals(3, ((InsertRowNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(4, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(4, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(5, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator02() throws Exception {
    simulateFileScenario01();

    IndexedConsensusRequest request;
    PlanNode planNode;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(4);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(4, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(4, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());

    // wait for next
    ExecutorService checkThread = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        checkThread.submit(
            () -> {
              iterator.waitForNextReady();
              Assert.assertTrue(iterator.hasNext());
              IndexedConsensusRequest req = iterator.next();
              Assert.assertEquals(1, req.getRequests().size());
              for (IConsensusRequest innerRequest : req.getRequests()) {
                PlanNode node =
                    WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
                Assert.assertTrue(node instanceof InsertTabletNode);
                Assert.assertEquals(5, ((InsertTabletNode) node).getSearchIndex());
              }
              return true;
            });

    Thread.sleep(500);
    walNode.rollWALFile();
    InsertRowNode insertRowNode = getInsertRowNode(devicePath);
    walNode.log(0, insertRowNode); // put -1 after 6
    Assert.assertTrue(future.get());
    checkThread.shutdown();
  }

  @Test
  public void scenario01TestGetReqIterator03() throws Exception {
    simulateFileScenario01();
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(5);

    Assert.assertFalse(iterator.hasNext());

    // wait for next
    ExecutorService checkThread = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        checkThread.submit(
            () -> {
              iterator.waitForNextReady();
              IndexedConsensusRequest request;
              PlanNode planNode;
              Assert.assertTrue(iterator.hasNext());
              request = iterator.next();
              Assert.assertEquals(1, request.getRequests().size());
              for (IConsensusRequest innerRequest : request.getRequests()) {
                planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
                Assert.assertTrue(planNode instanceof InsertTabletNode);
                Assert.assertEquals(5, ((InsertTabletNode) planNode).getSearchIndex());
              }
              iterator.waitForNextReady();
              Assert.assertTrue(iterator.hasNext());
              request = iterator.next();
              Assert.assertEquals(1, request.getRequests().size());
              for (IConsensusRequest innerRequest : request.getRequests()) {
                planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
                Assert.assertTrue(planNode instanceof InsertRowNode);
                Assert.assertEquals(6, ((InsertRowNode) planNode).getSearchIndex());
              }
              return true;
            });

    Thread.sleep(500);
    walNode.rollWALFile();
    InsertRowNode insertRowNode = getInsertRowNode(devicePath);
    walNode.log(0, insertRowNode); // put -1 after 6
    walNode.rollWALFile();
    insertRowNode = getInsertRowNode(devicePath);
    walNode.log(0, insertRowNode); // put -1 after 6
    Assert.assertTrue(future.get());
    checkThread.shutdown();
  }

  @Test
  public void scenario01TestGetReqIterator04() throws Exception {
    simulateFileScenario01();
    walNode.rollWALFile();

    IndexedConsensusRequest request;
    PlanNode planNode;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowNode);
      Assert.assertEquals(1, ((InsertRowNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowsNode);
      Assert.assertEquals(2, ((InsertRowsNode) planNode).getSearchIndex());
    }

    iterator.skipTo(4);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(4, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(4, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(5, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator05() throws Exception {
    simulateFileScenario01();
    walNode.rollWALFile();

    IndexedConsensusRequest request;
    PlanNode planNode;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(5);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(5, ((InsertTabletNode) planNode).getSearchIndex());
    }

    iterator.skipTo(2);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowsNode);
      Assert.assertEquals(2, ((InsertRowsNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertRowNode);
      Assert.assertEquals(3, ((InsertRowNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(4, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(4, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof InsertTabletNode);
      Assert.assertEquals(5, ((InsertTabletNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator06() throws Exception {
    simulateFileScenario01();
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(5);
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator07() throws Exception {
    simulateFileScenario01();
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(6);
    Assert.assertFalse(iterator.hasNext());
  }

  /**
   * Generate wal files as below: <br>
   * _0-0-0.wal: -1,-1 <br>
   * _1-0-0.wal: -1 <br>
   * _2-0-1.wal: -1,1 <br>
   * _3-1-0.wal: -1 <br>
   * 1 - DeleteDataNode
   */
  private void simulateFileScenario02() throws IllegalPathException {
    InsertRowNode insertRowNode = getInsertRowNode(devicePath);
    // _0-0-0.wal
    walNode.log(0, insertRowNode); // -1
    walNode.log(0, insertRowNode); // -1
    walNode.rollWALFile();
    // _1-0-0.wal
    walNode.log(0, insertRowNode); // -1
    walNode.rollWALFile();
    // _2-0-1.wal
    walNode.log(0, insertRowNode); // -1
    DeleteDataNode deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(1);
    walNode.log(0, deleteDataNode); // 1
    walNode.rollWALFile();
    // _3-1-0.wal
    walNode.log(0, insertRowNode); // -1
  }

  @Test
  public void scenario02TestGetReqIterator01() throws Exception {
    simulateFileScenario02();
    walNode.rollWALFile();
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    IndexedConsensusRequest request;
    PlanNode planNode;
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(1, ((DeleteDataNode) planNode).getSearchIndex());
    }
  }

  /**
   * Generate wal files as below: <br>
   * _0-0-1.wal: broken <br>
   * _1-0-1.wal: 1,-1 <br>
   * _2-1-1.wal: 2,2,2 <br>
   * _3-2-1.wal: 3,3 <br>
   * _4-3-1.wal: broken <br>
   * _5-3-1.wal: 3,5 <br>
   * _6-5-1.wal: broken <br>
   * _7-5-1.wal: broken <br>
   * _8-5-1.wal: broken <br>
   * _9-8-1.wal: 8 <br>
   * 1,2,3,5,8 - DeleteDataNode
   */
  private void simulateFileScenario03() throws IllegalPathException, IOException {
    // _0-0-1.wal
    walNode.rollWALFile();
    new File(logDirectory, "_0-0-0.wal").delete();
    new File(logDirectory, "_0-0-1.wal").createNewFile();
    // _1-0-1.wal
    DeleteDataNode deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(1);
    walNode.log(0, deleteDataNode); // 1
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(-1);
    walNode.log(0, deleteDataNode); // -1
    walNode.rollWALFile();
    // _2-1-1.wal
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(2);
    walNode.log(0, deleteDataNode); // 2
    walNode.log(0, deleteDataNode); // 2
    walNode.log(0, deleteDataNode); // 2
    walNode.rollWALFile();
    // _3-2-1.wal
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(3);
    walNode.log(0, deleteDataNode); // 3
    walNode.log(0, deleteDataNode); // 3
    walNode.rollWALFile();
    // _4-3-1.wal
    walNode.rollWALFile();
    new File(logDirectory, "_4-3-0.wal").delete();
    new File(logDirectory, "_4-3-1.wal").createNewFile();
    // _5-3-1.wal
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(3);
    walNode.log(0, deleteDataNode); // 3
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(5);
    walNode.log(0, deleteDataNode); // 5
    walNode.rollWALFile();
    // _6-5-1.wal
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(6);
    walNode.log(0, deleteDataNode); // 6
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(7);
    walNode.log(0, deleteDataNode); // 7
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(8);
    walNode.log(0, deleteDataNode); // 8
    walNode.rollWALFile();
    new File(logDirectory, "_6-5-1.wal").delete();
    new File(logDirectory, "_6-5-1.wal").createNewFile();
    // _7-5-1.wal
    walNode.rollWALFile();
    new File(logDirectory, "_7-5-0.wal").delete();
    new File(logDirectory, "_7-5-1.wal").createNewFile();
    // _8-5-1.wal
    walNode.rollWALFile();
    new File(logDirectory, "_8-5-0.wal").delete();
    new File(logDirectory, "_8-5-1.wal").createNewFile();
    // _9-8-1.wal
    deleteDataNode = getDeleteDataNode(devicePath);
    deleteDataNode.setSearchIndex(8);
    walNode.log(0, deleteDataNode); // 8
  }

  @Test
  public void scenario03TestGetReqIterator01() throws Exception {
    simulateFileScenario03();
    walNode.rollWALFile();

    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    IndexedConsensusRequest request;
    PlanNode planNode;

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(1, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(2, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(3, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(5, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
    walNode.rollWALFile();
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(8, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario03TestGetReqIterator02() throws Exception {
    simulateFileScenario03();
    walNode.rollWALFile();

    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(6);
    IndexedConsensusRequest request;
    PlanNode planNode;

    Assert.assertFalse(iterator.hasNext());
    walNode.rollWALFile();
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(8, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());

    iterator.skipTo(3);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(3, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(3, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(5, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next();
    Assert.assertEquals(1, request.getRequests().size());
    for (IConsensusRequest innerRequest : request.getRequests()) {
      planNode = WALEntry.deserializeForConsensus(innerRequest.serializeToByteBuffer());
      Assert.assertTrue(planNode instanceof DeleteDataNode);
      Assert.assertEquals(8, ((DeleteDataNode) planNode).getSearchIndex());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  private static InsertRowNode getInsertRowNode(String devicePath) throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });
    return insertRowNode;
  }

  private static InsertRowsNode getInsertRowsNode(String devicePath) throws IllegalPathException {
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    long time = 111L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
        };
    insertRowNode.setMeasurementSchemas(schemas);
    insertRowsNode.addOneInsertRowNode(insertRowNode, 0);

    time = 112L;
    insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);

    insertRowNode.setMeasurementSchemas(schemas);

    insertRowsNode.addOneInsertRowNode(insertRowNode, 1);

    return insertRowsNode;
  }

  private InsertTabletNode getInsertTabletNode(String devicePath, long[] times)
      throws IllegalPathException {
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = new double[times.length];
    columns[1] = new float[times.length];
    columns[2] = new long[times.length];
    columns[3] = new int[times.length];
    columns[4] = new boolean[times.length];
    columns[5] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((double[]) columns[0])[r] = 1.0 + r;
      ((float[]) columns[1])[r] = 2 + r;
      ((long[]) columns[2])[r] = 10000 + r;
      ((int[]) columns[3])[r] = 100 + r;
      ((boolean[]) columns[4])[r] = (r % 2 == 0);
      ((Binary[]) columns[5])[r] = new Binary("hh" + r, TSFileConfig.STRING_CHARSET);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    return new InsertTabletNode(
        new PlanNodeId(""),
        new PartialPath(devicePath),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        },
        times,
        bitMaps,
        columns,
        times.length);
  }

  private DeleteDataNode getDeleteDataNode(String devicePath) throws IllegalPathException {
    return new DeleteDataNode(
        new PlanNodeId(""),
        Collections.singletonList(new MeasurementPath(devicePath, "**")),
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }
}
