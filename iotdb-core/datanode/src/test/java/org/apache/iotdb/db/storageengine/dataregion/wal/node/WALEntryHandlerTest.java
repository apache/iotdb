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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.CheckpointManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.MemTablePinException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALInsertNodeCache;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WALEntryHandlerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier1 = String.valueOf(Integer.MAX_VALUE);
  private static final String identifier2 = String.valueOf(Integer.MAX_VALUE - 1);
  private static final String logDirectory1 =
      TestConstant.BASE_OUTPUT_PATH.concat("wal-test" + identifier1);
  private static final String logDirectory2 =
      TestConstant.BASE_OUTPUT_PATH.concat("wal-test" + identifier2);

  private static final String databasePath = "root.test_sg";
  private static final String devicePath = databasePath + ".test_d";
  private static final String dataRegionId = "1";
  private WALMode prevMode;
  private WALNode walNode1;
  private WALNode walNode2;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory1);
    EnvironmentUtils.cleanDir(logDirectory2);
    prevMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    walNode1 = new WALNode(identifier1, logDirectory1);
    walNode2 = new WALNode(identifier2, logDirectory2);
  }

  @After
  public void tearDown() throws Exception {
    walNode1.close();
    walNode2.close();
    config.setWalMode(prevMode);
    EnvironmentUtils.cleanDir(logDirectory1);
    EnvironmentUtils.cleanDir(logDirectory2);
    WALInsertNodeCache.getInstance(1).clear();
  }

  @Test(expected = MemTablePinException.class)
  public void pinDeletedMemTable1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
  }

  @Test(expected = MemTablePinException.class)
  public void pinDeletedMemTable2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowsNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
  }

  @Test
  public void pinMemTable1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    // roll wal file
    walNode1.rollWALFile();
    InsertRowNode node2 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node2.setSearchIndex(2);
    walNode1.log(memTable.getMemTableId(), node2);
    walNode1.onMemTableFlushed(memTable);
    walNode1.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // try to delete flushed but pinned memTable
    walNode1.deleteOutdatedFiles();
    // try to find node1
    itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
  }

  @Test
  public void pinMemTable2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowsNode node1 = getInsertRowsNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    // roll wal file
    walNode1.rollWALFile();
    InsertRowsNode node2 = getInsertRowsNode(devicePath, System.currentTimeMillis());
    node2.setSearchIndex(2);
    walNode1.log(memTable.getMemTableId(), node2);
    walNode1.onMemTableFlushed(memTable);
    walNode1.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // try to delete flushed but pinned memTable
    walNode1.deleteOutdatedFiles();
    // try to find node1
    itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
  }

  @Test(expected = MemTablePinException.class)
  public void unpinDeletedMemTable1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.unpinMemTable();
  }

  @Test(expected = MemTablePinException.class)
  public void unpinDeletedMemTable2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowsNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.unpinMemTable();
  }

  @Test
  public void unpinFlushedMemTable1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    // pin twice
    handler.pinMemTable();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    // unpin 1
    CheckpointManager checkpointManager = walNode1.getCheckpointManager();
    handler.unpinMemTable();
    MemTableInfo oldestMemTableInfo = checkpointManager.getOldestUnpinnedMemTableInfo();
    assertNull(oldestMemTableInfo);
    // unpin 2
    handler.unpinMemTable();
    assertNull(checkpointManager.getOldestUnpinnedMemTableInfo());
  }

  @Test
  public void unpinFlushedMemTable2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowsNode(devicePath, System.currentTimeMillis()));
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    // pin twice
    handler.pinMemTable();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    // unpin 1
    CheckpointManager checkpointManager = walNode1.getCheckpointManager();
    handler.unpinMemTable();
    MemTableInfo oldestMemTableInfo = checkpointManager.getOldestUnpinnedMemTableInfo();
    assertNull(oldestMemTableInfo);
    // unpin 2
    handler.unpinMemTable();
    assertNull(checkpointManager.getOldestUnpinnedMemTableInfo());
  }

  @Test
  public void unpinMemTable1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // roll wal file
    walNode1.rollWALFile();
    walNode1.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // unpin flushed memTable
    handler.unpinMemTable();
    // try to delete flushed but pinned memTable
    walNode1.deleteOutdatedFiles();
    // try to find node1
    itr = walNode1.getReqIterator(1);
    assertFalse(itr.hasNext());
  }

  @Test
  public void unpinMemTable2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowsNode node1 = getInsertRowsNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // roll wal file
    walNode1.rollWALFile();
    walNode1.rollWALFile();
    // find node1
    ConsensusReqReader.ReqIterator itr = walNode1.getReqIterator(1);
    assertTrue(itr.hasNext());
    assertEquals(
        node1,
        WALEntry.deserializeForConsensus(itr.next().getRequests().get(0).serializeToByteBuffer()));
    // unpin flushed memTable
    handler.unpinMemTable();
    // try to delete flushed but pinned memTable
    walNode1.deleteOutdatedFiles();
    // try to find node1
    itr = walNode1.getReqIterator(1);
    assertFalse(itr.hasNext());
  }

  @Test
  public void getUnFlushedValue1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    assertEquals(node1, handler.getInsertNode());
  }

  @Test
  public void getUnFlushedValue2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowsNode node1 = getInsertRowsNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    assertEquals(node1, handler.getInsertNode());
  }

  @Test
  public void getFlushedValue1() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // wait until wal flushed
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    assertEquals(node1, handler.getInsertNode());
  }

  @Test
  public void getFlushedValue2() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowsNode node1 = getInsertRowsNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // wait until wal flushed
    Awaitility.await().until(() -> walNode1.isAllWALEntriesConsumed());
    assertEquals(node1, handler.getInsertNode());
  }

  @Test
  public void testConcurrentGetValue1() throws Exception {
    int threadsNum = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadsNum; ++i) {
      WALNode walNode = i % 2 == 0 ? walNode1 : walNode2;
      String logDirectory = i % 2 == 0 ? logDirectory1 : logDirectory2;
      Callable<Void> writeTask =
          () -> {
            IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
            walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");

            List<WALFlushListener> walFlushListeners = new ArrayList<>();
            List<InsertRowNode> expectedInsertRowNodes = new ArrayList<>();
            try {
              for (int j = 0; j < 1_000; ++j) {
                long memTableId = memTable.getMemTableId();
                InsertRowNode node =
                    getInsertRowNode(devicePath + memTableId, System.currentTimeMillis());
                expectedInsertRowNodes.add(node);
                WALFlushListener walFlushListener = walNode.log(memTableId, node);
                walFlushListeners.add(walFlushListener);
              }
            } catch (IllegalPathException e) {
              fail();
            }

            // wait until wal flushed
            Awaitility.await().until(walNode::isAllWALEntriesConsumed);

            walFlushListeners.get(0).getWalEntryHandler().pinMemTable();
            walNode.onMemTableFlushed(memTable);

            for (int j = 0; j < expectedInsertRowNodes.size(); ++j) {
              InsertRowNode expect = expectedInsertRowNodes.get(j);
              InsertRowNode actual =
                  (InsertRowNode) walFlushListeners.get(j).getWalEntryHandler().getInsertNode();
              assertEquals(expect, actual);
            }

            walFlushListeners.get(0).getWalEntryHandler().unpinMemTable();
            return null;
          };
      Future<Void> future = executorService.submit(writeTask);
      futures.add(future);
    }
    // wait until all write tasks are done
    for (Future<Void> future : futures) {
      future.get();
    }
    executorService.shutdown();
  }

  @Test
  public void testConcurrentGetValue2() throws Exception {
    int threadsNum = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadsNum; ++i) {
      WALNode walNode = i % 2 == 0 ? walNode1 : walNode2;
      String logDirectory = i % 2 == 0 ? logDirectory1 : logDirectory2;
      Callable<Void> writeTask =
          () -> {
            IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
            walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");

            List<WALFlushListener> walFlushListeners = new ArrayList<>();
            List<InsertRowsNode> expectedInsertRowsNodes = new ArrayList<>();
            try {
              for (int j = 0; j < 1_000; ++j) {
                long memTableId = memTable.getMemTableId();
                InsertRowsNode node =
                    getInsertRowsNode(devicePath + memTableId, System.currentTimeMillis());
                expectedInsertRowsNodes.add(node);
                WALFlushListener walFlushListener = walNode.log(memTableId, node);
                walFlushListeners.add(walFlushListener);
              }
            } catch (IllegalPathException e) {
              fail();
            }

            // wait until wal flushed
            Awaitility.await().until(walNode::isAllWALEntriesConsumed);

            walFlushListeners.get(0).getWalEntryHandler().pinMemTable();
            walNode.onMemTableFlushed(memTable);

            for (int j = 0; j < expectedInsertRowsNodes.size(); ++j) {
              InsertRowsNode expect = expectedInsertRowsNodes.get(j);
              InsertRowsNode actual =
                  (InsertRowsNode) walFlushListeners.get(j).getWalEntryHandler().getInsertNode();
              assertEquals(expect, actual);
            }

            walFlushListeners.get(0).getWalEntryHandler().unpinMemTable();
            return null;
          };
      Future<Void> future = executorService.submit(writeTask);
      futures.add(future);
    }
    // wait until all write tasks are done
    for (Future<Void> future : futures) {
      future.get();
    }
    executorService.shutdown();
  }

  private InsertRowNode getInsertRowNode(String devicePath, long time) throws IllegalPathException {
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
    columns[0] = 1.0d;
    columns[1] = 2f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema("s" + (i + 1), dataTypes[i]);
    }
    node.setMeasurementSchemas(schemas);
    return node;
  }

  private InsertRowsNode getInsertRowsNode(String devicePath, long firstTime)
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
    columns[0] = 1.0d;
    columns[1] = 2f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            firstTime,
            columns,
            false);
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema("s" + (i + 1), dataTypes[i]);
    }
    node.setMeasurementSchemas(schemas);

    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    insertRowsNode.addOneInsertRowNode(node, 0);

    node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(devicePath),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            firstTime + 10,
            columns,
            false);
    schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema("s" + (i + 1), dataTypes[i]);
    }
    node.setMeasurementSchemas(schemas);
    insertRowsNode.addOneInsertRowNode(node, 1);
    return insertRowsNode;
  }
}
