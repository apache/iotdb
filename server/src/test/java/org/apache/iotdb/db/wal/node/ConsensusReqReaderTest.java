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
package org.apache.iotdb.db.wal.node;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
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
    // _0-0-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(1);
    walNode.log(0, insertRowNode); // 1
    insertTabletNode = getInsertTabletNode(devicePath, new long[] {2});
    walNode.log(0, insertTabletNode, 0, insertTabletNode.getRowCount()); // -1
    walNode.rollWALFile();
    // _1-1-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(2);
    walNode.log(0, insertRowNode); // 2
    walNode.log(0, insertRowNode); // 2
    walNode.log(0, insertRowNode); // 2
    walNode.rollWALFile();
    // _2-2-1.wal
    insertRowNode = getInsertRowNode(devicePath);
    insertRowNode.setSearchIndex(3);
    walNode.log(0, insertRowNode); // 3
    walNode.log(0, insertRowNode); // 3
    walNode.rollWALFile();
    // _3-3-1.wal
    insertRowNode.setDevicePath(new PartialPath(devicePath + "test"));
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
    WALFlushListener walFlushListener = walNode.log(0, insertRowNode); // 6
    walFlushListener.waitForResult();
  }

  @Test
  public void scenario01TestGetReq01() throws Exception {
    simulateFileScenario01();

    IConsensusRequest request;
    request = walNode.getReq(1);
    Assert.assertTrue(request instanceof InsertRowNode);
    Assert.assertEquals(1, ((InsertRowNode) request).getSearchIndex());
    request = walNode.getReq(2);
    Assert.assertTrue(request instanceof InsertRowsOfOneDeviceNode);
    Assert.assertEquals(2, ((InsertRowsOfOneDeviceNode) request).getSearchIndex());
    Assert.assertEquals(
        3, ((InsertRowsOfOneDeviceNode) request).getInsertRowNodeIndexList().size());
    request = walNode.getReq(3);
    Assert.assertTrue(request instanceof InsertRowsNode);
    Assert.assertEquals(3, ((InsertRowsNode) request).getSearchIndex());
    Assert.assertEquals(3, ((InsertRowsNode) request).getInsertRowNodeIndexList().size());
    request = walNode.getReq(4);
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    request = walNode.getReq(5);
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
    request = walNode.getReq(6);
    Assert.assertNull(request);
  }

  @Test
  public void scenario01TestGetReqs01() throws Exception {
    simulateFileScenario01();
    List<IConsensusRequest> requests;
    IConsensusRequest request;

    requests = walNode.getReqs(1, 6);
    Assert.assertEquals(5, requests.size());
    request = requests.get(0);
    Assert.assertTrue(request instanceof InsertRowNode);
    Assert.assertEquals(1, ((InsertRowNode) request).getSearchIndex());
    request = requests.get(1);
    Assert.assertTrue(request instanceof InsertRowsOfOneDeviceNode);
    Assert.assertEquals(2, ((InsertRowsOfOneDeviceNode) request).getSearchIndex());
    Assert.assertEquals(
        3, ((InsertRowsOfOneDeviceNode) request).getInsertRowNodeIndexList().size());
    request = requests.get(2);
    Assert.assertTrue(request instanceof InsertRowsNode);
    Assert.assertEquals(3, ((InsertRowsNode) request).getSearchIndex());
    Assert.assertEquals(3, ((InsertRowsNode) request).getInsertRowNodeIndexList().size());
    request = requests.get(3);
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    request = requests.get(4);
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
  }

  @Test
  public void scenario01TestGetReqs02() throws Exception {
    simulateFileScenario01();
    List<IConsensusRequest> requests;
    IConsensusRequest request;

    requests = walNode.getReqs(3, 1);
    Assert.assertEquals(1, requests.size());
    request = requests.get(0);
    Assert.assertTrue(request instanceof InsertRowsNode);
    Assert.assertEquals(3, ((InsertRowsNode) request).getSearchIndex());
    Assert.assertEquals(3, ((InsertRowsNode) request).getInsertRowNodeIndexList().size());
  }

  @Test
  public void scenario01TestGetReqs03() throws Exception {
    simulateFileScenario01();
    List<IConsensusRequest> requests;
    IConsensusRequest request;

    requests = walNode.getReqs(4, 2);
    Assert.assertEquals(2, requests.size());
    request = requests.get(0);
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    request = requests.get(1);
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
  }

  @Test
  public void scenario01TestGetReqs04() throws Exception {
    simulateFileScenario01();
    List<IConsensusRequest> requests;
    IConsensusRequest request;

    requests = walNode.getReqs(5, 100);
    Assert.assertEquals(1, requests.size());
    request = requests.get(0);
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
  }

  @Test
  public void scenario01TestGetReqs05() throws Exception {
    simulateFileScenario01();
    List<IConsensusRequest> requests;

    requests = walNode.getReqs(6, 100);
    Assert.assertEquals(0, requests.size());
  }

  @Test
  public void scenario01TestGetReqIterator01() throws Exception {
    simulateFileScenario01();
    IConsensusRequest request;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowNode);
    Assert.assertEquals(1, ((InsertRowNode) request).getSearchIndex());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowsOfOneDeviceNode);
    Assert.assertEquals(2, ((InsertRowsOfOneDeviceNode) request).getSearchIndex());
    Assert.assertEquals(
        3, ((InsertRowsOfOneDeviceNode) request).getInsertRowNodeIndexList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowsNode);
    Assert.assertEquals(3, ((InsertRowsNode) request).getSearchIndex());
    Assert.assertEquals(3, ((InsertRowsNode) request).getInsertRowNodeIndexList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator02() throws Exception {
    simulateFileScenario01();
    IConsensusRequest request;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(4);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());

    Assert.assertFalse(iterator.hasNext());
    // wait for next
    ExecutorService checkThread = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        checkThread.submit(
            () -> {
              iterator.waitForNextReady();
              Assert.assertTrue(iterator.hasNext());
              IConsensusRequest req = iterator.next().getRequest();
              ;
              Assert.assertTrue(req instanceof InsertRowNode);
              Assert.assertEquals(6, ((InsertRowNode) req).getSearchIndex());
              return true;
            });

    Thread.sleep(500);
    InsertRowNode insertRowNode = getInsertRowNode(devicePath);
    walNode.log(0, insertRowNode); // put -1 after 6
    Assert.assertTrue(future.get());
  }

  @Test
  public void scenario01TestGetReqIterator03() throws Exception {
    simulateFileScenario01();
    IConsensusRequest request;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(5);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());

    Assert.assertFalse(iterator.hasNext());
    // wait for next
    ExecutorService checkThread = Executors.newSingleThreadExecutor();
    Future<Boolean> future =
        checkThread.submit(
            () -> {
              iterator.waitForNextReady();
              Assert.assertTrue(iterator.hasNext());
              IConsensusRequest req = iterator.next().getRequest();
              ;
              Assert.assertTrue(req instanceof InsertRowNode);
              Assert.assertEquals(6, ((InsertRowNode) req).getSearchIndex());
              return true;
            });

    Thread.sleep(500);
    walNode.rollWALFile();
    InsertRowNode insertRowNode = getInsertRowNode(devicePath);
    walNode.log(0, insertRowNode); // put -1 after 6
    Assert.assertTrue(future.get());
  }

  @Test
  public void scenario01TestGetReqIterator04() throws Exception {
    simulateFileScenario01();
    IConsensusRequest request;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowNode);
    Assert.assertEquals(1, ((InsertRowNode) request).getSearchIndex());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowsOfOneDeviceNode);
    Assert.assertEquals(2, ((InsertRowsOfOneDeviceNode) request).getSearchIndex());
    Assert.assertEquals(
        3, ((InsertRowsOfOneDeviceNode) request).getInsertRowNodeIndexList().size());

    iterator.skipTo(4);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void scenario01TestGetReqIterator05() throws Exception {
    simulateFileScenario01();
    IConsensusRequest request;
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(5);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());

    iterator.skipTo(2);

    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowsOfOneDeviceNode);
    Assert.assertEquals(2, ((InsertRowsOfOneDeviceNode) request).getSearchIndex());
    Assert.assertEquals(
        3, ((InsertRowsOfOneDeviceNode) request).getInsertRowNodeIndexList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertRowsNode);
    Assert.assertEquals(3, ((InsertRowsNode) request).getSearchIndex());
    Assert.assertEquals(3, ((InsertRowsNode) request).getInsertRowNodeIndexList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertMultiTabletsNode);
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getSearchIndex());
    Assert.assertEquals(4, ((InsertMultiTabletsNode) request).getInsertTabletNodeList().size());
    Assert.assertTrue(iterator.hasNext());
    request = iterator.next().getRequest();
    Assert.assertTrue(request instanceof InsertTabletNode);
    Assert.assertEquals(5, ((InsertTabletNode) request).getSearchIndex());
    Assert.assertFalse(iterator.hasNext());
  }

  public static InsertRowNode getInsertRowNode(String devicePath) throws IllegalPathException {
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
    columns[5] = new Binary("hh" + 0);

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
      ((Binary[]) columns[5])[r] = new Binary("hh" + r);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            times,
            bitMaps,
            columns,
            times.length);

    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });

    return insertTabletNode;
  }
}
