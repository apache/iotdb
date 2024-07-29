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
package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WALInsertNodeCacheTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String databasePath = "root.test_sg";
  private static final String devicePath = databasePath + ".test_d";
  private static final String dataRegionId = "1";
  private static final WALInsertNodeCache cache = WALInsertNodeCache.getInstance(1);
  private WALMode prevMode;
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    cache.clear();
    prevMode = config.getWalMode();
    config.setWalMode(WALMode.SYNC);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    cache.clear();
    config.setWalMode(prevMode);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  @Test
  public void testLoadAfterSyncBuffer() throws IllegalPathException {
    try {
      // Limit the wal buffer size to trigger sync Buffer when writing wal entry
      walNode.setBufferSize(24);
      // write memTable
      IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
      walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
      InsertRowNode node1 = getInsertRowNode(System.currentTimeMillis());
      node1.setSearchIndex(1);
      WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
      WALEntryPosition position = flushListener.getWalEntryHandler().getWalEntryPosition();
      // wait until wal flushed
      walNode.rollWALFile();
      Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed() && position.canRead());
      // load by cache
      System.out.println(position.getPosition());
      assertEquals(node1, cache.getInsertNode(position));
    } finally {
      walNode.setBufferSize(config.getWalBufferSize());
    }
  }

  @Test
  public void testGetInsertNodeInParallel() throws IllegalPathException {
    // write memTable
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    WALEntryPosition position = flushListener.getWalEntryHandler().getWalEntryPosition();
    // wait until wal flushed
    walNode.rollWALFile();
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed() && position.canRead());
    // Test getInsertNode in parallel to detect buffer concurrent problem
    AtomicBoolean failure = new AtomicBoolean(false);
    List<Thread> threadList = new ArrayList<>(5);
    for (int i = 0; i < 5; ++i) {
      Thread getInsertNodeThread =
          new Thread(
              () -> {
                if (!node1.equals(cache.getInsertNode(position))) {
                  failure.set(true);
                }
              });
      threadList.add(getInsertNodeThread);
      getInsertNodeThread.start();
    }
    Awaitility.await()
        .until(
            () -> {
              for (Thread thread : threadList) {
                if (thread.isAlive()) {
                  return false;
                }
              }
              return true;
            });
    assertFalse(failure.get());
  }

  @Test
  public void testLoadUnsealedWALFile() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + "/" + "fake.tsfile");
    InsertRowNode node1 = getInsertRowNode(System.currentTimeMillis());
    node1.setSearchIndex(1);
    WALFlushListener flushListener = walNode.log(memTable.getMemTableId(), node1);
    WALEntryPosition position = flushListener.getWalEntryHandler().getWalEntryPosition();
    // wait until wal flushed
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed() && position.canRead());
    // load by cache
    assertEquals(node1, cache.getInsertNode(position));
  }

  @Test
  public void testBatchLoad() throws Exception {
    // Enable batch load
    boolean oldIsBatchLoadEnabled = cache.isBatchLoadEnabled();
    cache.setIsBatchLoadEnabled(true);
    WALInsertNodeCache localC = cache;
    try {
      // write memTable1
      IMemTable memTable1 = new PrimitiveMemTable(databasePath, dataRegionId);
      walNode.onMemTableCreated(memTable1, logDirectory + "/" + "fake1.tsfile");
      InsertRowNode node1 = getInsertRowNode(System.currentTimeMillis());
      node1.setSearchIndex(1);
      WALFlushListener flushListener1 = walNode.log(memTable1.getMemTableId(), node1);
      WALEntryPosition position1 = flushListener1.getWalEntryHandler().getWalEntryPosition();
      InsertRowNode node2 = getInsertRowNode(System.currentTimeMillis());
      node1.setSearchIndex(2);
      WALFlushListener flushListener2 = walNode.log(memTable1.getMemTableId(), node2);
      WALEntryPosition position2 = flushListener2.getWalEntryHandler().getWalEntryPosition();
      // write memTable2
      IMemTable memTable2 = new PrimitiveMemTable(databasePath, dataRegionId);
      walNode.onMemTableCreated(memTable2, logDirectory + "/" + "fake2.tsfile");
      InsertRowNode node3 = getInsertRowNode(System.currentTimeMillis());
      node1.setSearchIndex(3);
      WALFlushListener flushListener3 = walNode.log(memTable2.getMemTableId(), node3);
      WALEntryPosition position3 = flushListener3.getWalEntryHandler().getWalEntryPosition();
      // wait until wal flushed
      walNode.rollWALFile();
      Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed() && position3.canRead());
      // check batch load memTable1
      cache.clear();
      cache.addMemTable(memTable1.getMemTableId());
      assertEquals(node1, cache.getInsertNode(position1));
      assertTrue(cache.contains(position1));
      assertTrue(cache.contains(position2));
      assertFalse(cache.contains(position3));
      // check batch load none
      cache.removeMemTable(memTable1.getMemTableId());
      cache.clear();
      assertEquals(node1, cache.getInsertNode(position1));
      assertTrue(cache.contains(position1));
      assertFalse(cache.contains(position2));
      assertFalse(cache.contains(position3));
    } finally {
      WALInsertNodeCache.getInstance(1).setIsBatchLoadEnabled(oldIsBatchLoadEnabled);
    }
  }

  private InsertRowNode getInsertRowNode(long time) throws IllegalPathException {
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
            new PartialPath(WALInsertNodeCacheTest.devicePath),
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
