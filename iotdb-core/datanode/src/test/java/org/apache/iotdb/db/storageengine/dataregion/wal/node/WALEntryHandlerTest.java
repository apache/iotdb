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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

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

  private static final String devicePath = "root.test_sg.test_d";
  private WALMode prevMode;
  private boolean prevIsClusterMode;
  private WALNode walNode1;
  private WALNode walNode2;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory1);
    EnvironmentUtils.cleanDir(logDirectory2);
    prevMode = config.getWalMode();
    prevIsClusterMode = config.isClusterMode();
    config.setWalMode(WALMode.SYNC);
    config.setClusterMode(true);
    walNode1 = new WALNode(identifier1, logDirectory1);
    walNode2 = new WALNode(identifier2, logDirectory2);
  }

  @After
  public void tearDown() throws Exception {
    walNode1.close();
    walNode2.close();
    config.setWalMode(prevMode);
    config.setClusterMode(prevIsClusterMode);
    EnvironmentUtils.cleanDir(logDirectory1);
    EnvironmentUtils.cleanDir(logDirectory2);
    WALInsertNodeCache.getInstance().clear();
  }

  @Test(expected = MemTablePinException.class)
  public void pinDeletedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
  }

  @Test
  public void pinMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
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
  public void unpinDeletedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    walNode1.onMemTableFlushed(memTable);
    // pin flushed memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.unpinMemTable();
  }

  @Test
  public void unpinFlushedMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    WALFlushListener flushListener =
        walNode1.log(
            memTable.getMemTableId(), getInsertRowNode(devicePath, System.currentTimeMillis()));
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    // pin twice
    handler.pinMemTable();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // unpin 1
    CheckpointManager checkpointManager = walNode1.getCheckpointManager();
    handler.unpinMemTable();
    MemTableInfo oldestMemTableInfo = checkpointManager.getOldestMemTableInfo();
    assertEquals(memTable.getMemTableId(), oldestMemTableInfo.getMemTableId());
    assertNull(oldestMemTableInfo.getMemTable());
    assertTrue(oldestMemTableInfo.isPinned());
    // unpin 2
    handler.unpinMemTable();
    assertNull(checkpointManager.getOldestMemTableInfo());
  }

  @Test
  public void unpinMemTable() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
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
  public void getUnFlushedValue() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    assertEquals(node1, handler.getValue());
  }

  @Test
  public void getFlushedValue() throws Exception {
    IMemTable memTable = new PrimitiveMemTable();
    walNode1.onMemTableCreated(memTable, logDirectory1 + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(devicePath, System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode1.log(memTable.getMemTableId(), node1);
    // pin memTable
    WALEntryHandler handler = flushListener.getWalEntryHandler();
    handler.pinMemTable();
    walNode1.onMemTableFlushed(memTable);
    // wait until wal flushed
    while (!walNode1.isAllWALEntriesConsumed()) {
      Thread.sleep(50);
    }
    assertEquals(node1, handler.getValue());
  }

  @Test
  public void testConcurrentGetValue() throws Exception {
    int threadsNum = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadsNum; ++i) {
      WALNode walNode = i % 2 == 0 ? walNode1 : walNode2;
      String logDirectory = i % 2 == 0 ? logDirectory1 : logDirectory2;
      Callable<Void> writeTask =
          () -> {
            IMemTable memTable = new PrimitiveMemTable();
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
            while (!walNode1.isAllWALEntriesConsumed() && !walNode2.isAllWALEntriesConsumed()) {
              Thread.sleep(50);
            }

            walFlushListeners.get(0).getWalEntryHandler().pinMemTable();
            walNode.onMemTableFlushed(memTable);

            for (int j = 0; j < expectedInsertRowNodes.size(); ++j) {
              InsertRowNode expect = expectedInsertRowNodes.get(j);
              InsertRowNode actual =
                  (InsertRowNode) walFlushListeners.get(j).getWalEntryHandler().getValue();
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
    columns[5] = new Binary("hh" + 0);

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
}
