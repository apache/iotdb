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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ContinuousSameSearchIndexSeparatorNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WALNodeWaitForRollFileTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory =
      TestConstant.BASE_OUTPUT_PATH.concat("wal-roll-file-test");
  private static final String databasePath = "root.test_sg";
  private static final String devicePath = databasePath + ".test_d";
  private static final String dataRegionId = "1";
  private WALMode prevMode;
  private String prevConsensus;
  private WALNode walNode;
  private long originWALThreshold;

  @Before
  public void setUp() throws Exception {
    originWALThreshold = config.getWalFileSizeThresholdInByte();
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    prevConsensus = config.getDataRegionConsensusProtocolClass();
    config.setWalMode(WALMode.SYNC);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    config.setWalFileSizeThresholdInByte(2 * 1024 * 1024);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    walNode.close();
    config.setWalMode(prevMode);
    config.setDataRegionConsensusProtocolClass(prevConsensus);
    config.setWalFileSizeThresholdInByte(originWALThreshold);
    EnvironmentUtils.cleanDir(logDirectory);
    StorageEngine.getInstance().reset();
  }

  /**
   * Verifies that waitForNextReady(time, unit) throws TimeoutException when no WAL data is
   * available at the requested search index. This uses waitForRollFile internally.
   */
  @Test
  public void testWaitForNextReadyTimesOutWhenNoData() throws Exception {
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    assertFalse(iterator.hasNext());
    try {
      iterator.waitForNextReady(1, TimeUnit.SECONDS);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) {
      // expected
    }
  }

  /**
   * Verifies that waitForNextReady(time, unit) does NOT wake up from a buffer flush alone — it
   * requires a WAL file roll. This is the core behavioral change: the old waitForFlush would return
   * on any buffer sync, but waitForRollFile only returns when a new WAL file is created.
   */
  @Test
  public void testWaitForNextReadyNotWokenByFlushWithoutRoll() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

    // write a small amount of data (not enough to trigger roll)
    InsertTabletNode insertTabletNode = getInsertTabletNode(devicePath, new long[] {1});
    insertTabletNode.setSearchIndex(1);
    walNode.log(
        memTable.getMemTableId(),
        insertTabletNode,
        Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    // data is flushed to buffer but no WAL file roll happened yet, iterator at search index 1
    // should not find data (because the current-writing WAL file is not readable by the iterator)
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    try {
      long start = System.currentTimeMillis();
      iterator.waitForNextReady(2, TimeUnit.SECONDS);
      if (System.currentTimeMillis() - start
          < WALNode.WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC * 1000) {
        fail("The data should not be found before timeout");
      }
    } catch (TimeoutException e) {
      // expected: flush happened but no roll, so waitForRollFile timed out
    }
  }

  /**
   * Verifies that waitForNextReady succeeds after a WAL file roll makes data readable. The iterator
   * should wake up when rollLogWriter signals the rollLogWriterCondition.
   */
  @Test
  public void testWaitForNextReadySucceedsAfterRollFile() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

    // write data with search index
    for (int i = 1; i <= 5; i++) {
      InsertTabletNode insertTabletNode = getInsertTabletNode(devicePath, new long[] {i});
      insertTabletNode.setSearchIndex(i);
      walNode.log(
          memTable.getMemTableId(),
          insertTabletNode,
          Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));
    }

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    // roll the WAL file so the data is in a closed file readable by the iterator
    walNode.rollWALFile();
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    // iterator at search index 1 should find the data after roll
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
  }

  @Test
  public void testLegacySeparatorStillWorksAfterRollFile() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

    InsertTabletNode insertTabletNode = getInsertTabletNode(devicePath, new long[] {1});
    insertTabletNode.setSearchIndex(1);
    walNode.log(
        memTable.getMemTableId(),
        insertTabletNode,
        Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));
    walNode.log(memTable.getMemTableId(), new ContinuousSameSearchIndexSeparatorNode());

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    walNode.rollWALFile();
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    assertTrue(iterator.hasNext());
    assertNotNull(iterator.next());
  }

  /**
   * Verifies that waitForNextReady wakes up when a WAL file roll is triggered concurrently. A
   * background thread rolls the WAL file while the main thread waits on the iterator.
   */
  @Test
  public void testWaitForNextReadyWakesUpOnConcurrentRoll() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

    // write data with search index
    InsertTabletNode insertTabletNode = getInsertTabletNode(devicePath, new long[] {1});
    insertTabletNode.setSearchIndex(1);
    insertTabletNode.setLastFragment(true);
    walNode.log(
        memTable.getMemTableId(),
        insertTabletNode,
        Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch waiterStarted = new CountDownLatch(1);
    try {
      // background: wait for data to become available via waitForNextReady
      Future<Boolean> waitFuture =
          executor.submit(
              () -> {
                waiterStarted.countDown();
                iterator.waitForNextReady(15, TimeUnit.SECONDS);
                return iterator.hasNext();
              });

      assertTrue(waiterStarted.await(10, TimeUnit.SECONDS));

      // trigger WAL file roll — this should signal rollLogWriterCondition and wake up the iterator
      walNode.rollWALFile();
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> walNode.isAllWALEntriesConsumed());

      assertTrue(
          "Iterator should have found data after WAL file roll",
          waitFuture.get(20, TimeUnit.SECONDS));
    } finally {
      shutdownExecutor(executor);
    }
  }

  /**
   * Verifies that the no-arg waitForNextReady eventually proceeds when enough data is written to
   * trigger an automatic WAL file roll (file size exceeds threshold). Uses a small WAL file size
   * threshold to trigger the roll quickly.
   */
  @Test
  public void testWaitForNextReadyWithAutoRollOnSizeThreshold() throws Exception {
    // use small WAL file size to trigger auto-roll
    config.setWalFileSizeThresholdInByte(1024);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch waiterStarted = new CountDownLatch(1);

    try {
      IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
      walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

      // write initial data with search index
      InsertTabletNode first = getInsertTabletNode(devicePath, new long[] {1});
      first.setSearchIndex(1);
      walNode.log(
          memTable.getMemTableId(),
          first,
          Collections.singletonList(new int[] {0, first.getRowCount()}));

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> walNode.isAllWALEntriesConsumed());

      ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);

      Future<Boolean> waitFuture =
          executor.submit(
              () -> {
                waiterStarted.countDown();
                iterator.waitForNextReady(30, TimeUnit.SECONDS);
                return iterator.hasNext();
              });

      assertTrue(waiterStarted.await(10, TimeUnit.SECONDS));

      // write more data to exceed the small threshold and trigger auto-roll
      for (int i = 2; i <= 50; i++) {
        InsertTabletNode node = getInsertTabletNode(devicePath, new long[] {i});
        node.setSearchIndex(i);
        walNode.log(
            memTable.getMemTableId(),
            node,
            Collections.singletonList(new int[] {0, node.getRowCount()}));
      }

      assertTrue(
          "Iterator should have found data after auto WAL file roll",
          waitFuture.get(40, TimeUnit.SECONDS));
    } finally {
      try {
        shutdownExecutor(executor);
      } finally {
        config.setWalFileSizeThresholdInByte(2 * 1024 * 1024);
      }
    }
  }

  /**
   * Verifies that the no-arg waitForNextReady() automatically triggers a WAL file roll after the
   * timeout expires (WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC = 30s). The flow is: data written to
   * buffer → waitForRollFile(30s) times out → rollWALFile() called → data moves to closed file →
   * hasNext() returns true → method returns.
   */
  @Test
  public void testWaitForNextReadyAutoTriggersRollOnTimeout() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, logDirectory + File.separator + "test.tsfile");

    // write data with search index — stays in the current (active) WAL file
    InsertTabletNode insertTabletNode = getInsertTabletNode(devicePath, new long[] {1});
    insertTabletNode.setSearchIndex(1);
    insertTabletNode.setLastFragment(true);
    walNode.log(
        memTable.getMemTableId(),
        insertTabletNode,
        Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));

    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> walNode.isAllWALEntriesConsumed());

    // iterator cannot read the active WAL file, so hasNext() should be false
    ConsensusReqReader.ReqIterator iterator = walNode.getReqIterator(1);
    assertFalse("Data should not be visible before WAL file roll", iterator.hasNext());

    ExecutorService executor = Executors.newSingleThreadExecutor();

    long startTime = System.currentTimeMillis();

    // call the no-arg waitForNextReady() — it should:
    // 1) wait 30s for rollLogWriterCondition (timeout)
    // 2) auto-call rollWALFile()
    // 3) data becomes readable, hasNext() returns true, method returns
    try {
      Future<Boolean> waitFuture =
          executor.submit(
              () -> {
                iterator.waitForNextReady();
                return iterator.hasNext();
              });

      assertTrue(
          "Iterator should have found data after auto-triggered WAL file roll",
          waitFuture.get(90, TimeUnit.SECONDS));

      long elapsed = System.currentTimeMillis() - startTime;
      assertTrue(
          "Should have waited at least 30s for the timeout to trigger auto-roll, but only waited "
              + elapsed
              + "ms",
          elapsed >= TimeUnit.SECONDS.toMillis(WALNode.WAIT_FOR_NEXT_WAL_ENTRY_TIMEOUT_IN_SEC - 1));
    } finally {
      shutdownExecutor(executor);
    }
  }

  private static void shutdownExecutor(final ExecutorService executor) throws InterruptedException {
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
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
      ((Binary[]) columns[5])[r] = new Binary("hh" + r, TSFileConfig.STRING_CHARSET);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      bitMaps[i].mark(i % times.length);
    }
    MeasurementSchema[] schemas = new MeasurementSchema[6];
    for (int i = 0; i < 6; i++) {
      schemas[i] = new MeasurementSchema(measurements[i], dataTypes[i], TSEncoding.PLAIN);
    }

    return new InsertTabletNode(
        new PlanNodeId(""),
        new PartialPath(devicePath),
        false,
        measurements,
        dataTypes,
        schemas,
        times,
        bitMaps,
        columns,
        times.length);
  }
}
