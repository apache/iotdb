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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALReader;
import org.apache.iotdb.db.wal.recover.CheckpointRecoverUtils;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WALNodeTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String devicePath = "root.test_sg.test_d";
  private WALMode prevMode;
  private WALNode walNode;

  private boolean isClusterMode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    isClusterMode = config.isClusterMode();
    config.setWalMode(WALMode.SYNC);
    config.setClusterMode(true);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    config.setWalMode(prevMode);
    config.setClusterMode(isClusterMode);
    EnvironmentUtils.cleanDir(logDirectory);
  }

  @Test
  public void testConcurrentWrite() throws Exception {
    // start write threads to write concurrently
    int threadsNum = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    List<WALFlushListener> walFlushListeners = Collections.synchronizedList(new ArrayList<>());
    Set<InsertTabletNode> expectedInsertTabletNodes = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < threadsNum; ++i) {
      int memTableId = i;
      Callable<Void> writeTask =
          () -> {
            try {
              writeInsertTabletNode(memTableId, expectedInsertTabletNodes, walFlushListeners);
            } catch (IllegalPathException e) {
              fail();
            }
            return null;
          };
      Future<Void> future = executorService.submit(writeTask);
      futures.add(future);
    }
    // wait until all write tasks are done
    for (Future<Void> future : futures) {
      future.get();
    }
    // wait a moment
    while (!walNode.isAllWALEntriesConsumed()) {
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    // check .wal files
    File[] walFiles = WALFileUtils.listAllWALFiles(new File(logDirectory));
    Set<InsertTabletNode> actualInsertTabletNodes = new HashSet<>();
    if (walFiles != null) {
      for (File walFile : walFiles) {
        try (WALReader walReader = new WALReader(walFile)) {
          while (walReader.hasNext()) {
            actualInsertTabletNodes.add((InsertTabletNode) walReader.next().getValue());
          }
        }
      }
    }
    assertEquals(expectedInsertTabletNodes, actualInsertTabletNodes);
    // check flush listeners
    try {
      for (WALFlushListener walFlushListener : walFlushListeners) {
        assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.waitForResult());
      }
    } catch (NullPointerException e) {
      // ignore
    }
  }

  private void writeInsertTabletNode(
      int memTableId,
      Set<InsertTabletNode> expectedInsertTabletNodes,
      List<WALFlushListener> walFlushListeners)
      throws IllegalPathException {
    for (int i = 0; i < 100; ++i) {
      InsertTabletNode insertTabletNode =
          getInsertTabletNode(devicePath + memTableId, new long[] {i});
      expectedInsertTabletNodes.add(insertTabletNode);
      WALFlushListener walFlushListener =
          walNode.log(memTableId, insertTabletNode, 0, insertTabletNode.getRowCount());
      walFlushListeners.add(walFlushListener);
    }
  }

  private InsertTabletNode getInsertTabletNode(String devicePath, long[] times)
      throws IllegalPathException {
    String[] measurements = new String[] {"s1", "s2", "s3", "s4", "s5", "s6"};
    TSDataType[] dataTypes = new TSDataType[6];
    dataTypes[0] = TSDataType.DOUBLE;
    dataTypes[1] = TSDataType.FLOAT;
    dataTypes[2] = TSDataType.INT64;
    dataTypes[3] = TSDataType.INT32;
    dataTypes[4] = TSDataType.BOOLEAN;
    dataTypes[5] = TSDataType.TEXT;

    Object[] columns = new Object[6];
    columns[0] = new double[times.length];
    columns[1] = new float[times.length];
    columns[2] = new long[times.length];
    columns[3] = new int[times.length];
    columns[4] = new boolean[times.length];
    columns[5] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((double[]) columns[0])[r] = 1.0d + r;
      ((float[]) columns[1])[r] = 2.0f + r;
      ((long[]) columns[2])[r] = 10000L + r;
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
            measurements,
            dataTypes,
            times,
            bitMaps,
            columns,
            times.length);
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema(measurements[i], dataTypes[i], TSEncoding.PLAIN);
    }
    insertTabletNode.setMeasurementSchemas(schemas);
    return insertTabletNode;
  }

  @Test
  public void testConcurrentCheckpoint() throws Exception {
    // start write threads to write concurrently
    int threadsNum = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    Map<Long, MemTableInfo> expectedMemTableId2Info = new ConcurrentHashMap<>();
    // create 10 memTables, and flush the first 5 of them
    int memTablesNum = 10;
    for (int i = 0; i < memTablesNum; ++i) {
      Callable<Void> writeTask =
          () -> {
            IMemTable memTable = new PrimitiveMemTable();
            long memTableId = memTable.getMemTableId();
            String tsFilePath = logDirectory + File.separator + memTableId + ".tsfile";
            long firstFileVersionId = walNode.getCurrentLogVersion();
            walNode.onMemTableCreated(memTable, tsFilePath);
            if (memTableId % 2 == 0) {
              walNode.onMemTableFlushed(memTable);
            } else {
              // mimic MemTableInfo
              MemTableInfo memTableInfo =
                  new MemTableInfo(memTable, tsFilePath, firstFileVersionId);
              expectedMemTableId2Info.put(memTableId, memTableInfo);
            }
            return null;
          };
      Future<Void> future = executorService.submit(writeTask);
      futures.add(future);
    }
    // wait until all write tasks are done
    for (Future<Void> future : futures) {
      future.get();
    }
    // recover info from checkpoint file
    Map<Long, MemTableInfo> actualMemTableId2Info =
        CheckpointRecoverUtils.recoverMemTableInfo(new File(logDirectory)).getMemTableId2Info();
    assertEquals(expectedMemTableId2Info, actualMemTableId2Info);
  }

  @Test
  public void testDeleteOutdatedFiles() throws Exception {
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    // write until log is rolled
    long time = 0;
    IMemTable memTable = new PrimitiveMemTable();
    long memTableId = memTable.getMemTableId();
    String tsFilePath = logDirectory + File.separator + memTableId + ".tsfile";
    walNode.onMemTableCreated(memTable, tsFilePath);
    while (walNode.getCurrentLogVersion() == 0) {
      ++time;
      InsertTabletNode insertTabletNode =
          getInsertTabletNode(devicePath + memTableId, new long[] {time});
      WALFlushListener walFlushListener =
          walNode.log(memTableId, insertTabletNode, 0, insertTabletNode.getRowCount());
      walFlushListeners.add(walFlushListener);
    }
    walNode.onMemTableFlushed(memTable);
    walNode.onMemTableCreated(new PrimitiveMemTable(), tsFilePath);
    // check existence of _0-0-0.wal file and _1-0-1.wal file
    assertTrue(
        new File(
                logDirectory
                    + File.separator
                    + WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX))
            .exists());
    assertTrue(
        new File(
                logDirectory
                    + File.separator
                    + WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .exists());
    walNode.deleteOutdatedFiles();
    assertFalse(
        new File(
                logDirectory
                    + File.separator
                    + WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_NONE_SEARCH_INDEX))
            .exists());
    assertTrue(
        new File(
                logDirectory
                    + File.separator
                    + WALFileUtils.getLogFileName(1, 0, WALFileStatus.CONTAINS_SEARCH_INDEX))
            .exists());
    // check flush listeners
    try {
      for (WALFlushListener walFlushListener : walFlushListeners) {
        assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.waitForResult());
      }
    } catch (NullPointerException e) {
      // ignore
    }
  }
}
