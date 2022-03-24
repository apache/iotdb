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

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.wal.io.WALReader;
import org.apache.iotdb.db.wal.io.WALWriter;
import org.apache.iotdb.db.wal.recover.CheckpointRecoverUtils;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class WALNodeTest {
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = "wal-test";
  private static final String devicePath = "root.test_sg.test_d";
  private WALNode walNode;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanDir(logDirectory);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    EnvironmentUtils.cleanDir(logDirectory);
  }

  @Test
  public void testConcurrentWrite() throws Exception {
    // start write threads to write concurrently
    int threadsNum = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    Set<InsertTabletPlan> expectedInsertTabletPlans = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < threadsNum; ++i) {
      int memTableId = i;
      Callable<Void> writeTask =
          () -> {
            try {
              writeInsertTabletPlan(memTableId, expectedInsertTabletPlans, walFlushListeners);
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
    while (!walNode.isAllWALEditConsumed()) {
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    // check .wal files
    File[] walFiles = new File(logDirectory).listFiles(WALWriter::walFilenameFilter);
    Set<InsertTabletPlan> actualInsertTabletPlans = new HashSet<>();
    if (walFiles != null) {
      for (File walFile : walFiles) {
        try (WALReader walReader = new WALReader(walFile)) {
          while (walReader.hasNext()) {
            actualInsertTabletPlans.add((InsertTabletPlan) walReader.next().getValue());
          }
        }
      }
    }
    assertEquals(expectedInsertTabletPlans, actualInsertTabletPlans);
    // check flush listeners
    for (WALFlushListener walFlushListener : walFlushListeners) {
      assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.getResult());
    }
  }

  private void writeInsertTabletPlan(
      int memTableId,
      Set<InsertTabletPlan> expectedInsertTabletPlans,
      List<WALFlushListener> walFlushListeners)
      throws IllegalPathException {
    for (int i = 0; i < 100; ++i) {
      InsertTabletPlan insertTabletPlan =
          getInsertTabletPlan(devicePath + memTableId, new long[] {i});
      expectedInsertTabletPlans.add(insertTabletPlan);
      WALFlushListener walFlushListener = walNode.log(memTableId, insertTabletPlan);
      walFlushListeners.add(walFlushListener);
    }
  }

  private InsertTabletPlan getInsertTabletPlan(String devicePath, long[] times)
      throws IllegalPathException {
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

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

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(devicePath),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setBitMaps(bitMaps);
    return insertTabletPlan;
  }

  @Test
  public void testConcurrentCheckpoint() throws Exception {
    // start write threads to write concurrently
    int threadsNum = 5;
    ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);
    List<Future<Void>> futures = new ArrayList<>();
    Map<Integer, MemTableInfo> expectedMemTableId2Info = new ConcurrentHashMap<>();
    // create 10 memTables, and flush the first 5 of them
    int memTablesNum = 10;
    for (int i = 0; i < memTablesNum; ++i) {
      Callable<Void> writeTask =
          () -> {
            IMemTable memTable = new PrimitiveMemTable();
            int memTableId = memTable.getMemTableId();
            String tsFilePath = logDirectory + File.separator + memTableId + ".tsfile";
            int firstFileVersionId = walNode.getCurrentLogVersion();
            walNode.onMemTableCreated(memTable, tsFilePath);
            if (memTableId % 2 == 0) {
              walNode.onFlushEnd(memTable);
            } else {
              // mimic MemTableInfo
              MemTableInfo memTableInfo =
                  new MemTableInfo(memTableId, tsFilePath, firstFileVersionId);
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
    // fsync to checkpoint file
    walNode.fsyncCheckpointFile();
    // recover info from checkpoint file
    Map<Integer, MemTableInfo> actualMemTableId2Info =
        CheckpointRecoverUtils.recoverMemTableInfo(new File(logDirectory));
    assertEquals(expectedMemTableId2Info, actualMemTableId2Info);
  }

  @Test
  public void testDeleteOutdatedFiles() throws Exception {
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    // write until log is rolled
    long time = 0;
    IMemTable memTable = new PrimitiveMemTable();
    int memTableId = memTable.getMemTableId();
    String tsFilePath = logDirectory + File.separator + memTableId + ".tsfile";
    walNode.onMemTableCreated(memTable, tsFilePath);
    while (walNode.getCurrentLogVersion() == 0) {
      ++time;
      InsertTabletPlan insertTabletPlan =
          getInsertTabletPlan(devicePath + memTableId, new long[] {time});
      WALFlushListener walFlushListener = walNode.log(memTableId, insertTabletPlan);
      walFlushListeners.add(walFlushListener);
    }
    walNode.onFlushEnd(memTable);
    // check existence of _0.wal file
    assertTrue(new File(logDirectory + File.separator + WALWriter.getLogFileName(0)).exists());
    assertTrue(new File(logDirectory + File.separator + WALWriter.getLogFileName(1)).exists());
    walNode.deleteOutdatedFiles();
    assertFalse(new File(logDirectory + File.separator + WALWriter.getLogFileName(0)).exists());
    assertTrue(new File(logDirectory + File.separator + WALWriter.getLogFileName(1)).exists());
    // check flush listeners
    for (WALFlushListener walFlushListener : walFlushListeners) {
      assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.getResult());
    }
  }
}
