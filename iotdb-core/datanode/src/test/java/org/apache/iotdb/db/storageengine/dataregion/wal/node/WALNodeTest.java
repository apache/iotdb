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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.MemTableInfo;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.CheckpointRecoverUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
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
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WALNodeTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String identifier = String.valueOf(Integer.MAX_VALUE);
  private static final String logDirectory = TestConstant.BASE_OUTPUT_PATH.concat("wal-test");
  private static final String databasePath = "root.test_sg";
  private static final String devicePath = databasePath + ".test_d";
  private static final String dataRegionId = "1";
  private WALMode prevMode;
  private String prevConsensus;
  private WALNode walNode;
  private long originWALThreshold =
      IoTDBDescriptor.getInstance().getConfig().getWalFileSizeThresholdInByte();

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setWalFileSizeThresholdInByte(2 * 1024 * 1024);
    EnvironmentUtils.cleanDir(logDirectory);
    prevMode = config.getWalMode();
    prevConsensus = config.getDataRegionConsensusProtocolClass();
    config.setWalMode(WALMode.SYNC);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    walNode = new WALNode(identifier, logDirectory);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setWalFileSizeThresholdInByte(originWALThreshold);
    walNode.close();
    config.setWalMode(prevMode);
    config.setDataRegionConsensusProtocolClass(prevConsensus);
    EnvironmentUtils.cleanDir(logDirectory);
    StorageEngine.getInstance().reset();
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
    executorService.shutdown();
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
  }

  @Test
  public void testGetRetentionBoundaryByLastModifiedTime() throws Exception {
    final String timeRetentionDirectory = logDirectory + File.separator + "time-retention";
    EnvironmentUtils.cleanDir(timeRetentionDirectory);

    final long now = System.currentTimeMillis();
    createWalFile(
        timeRetentionDirectory,
        0,
        0,
        now - TimeUnit.HOURS.toMillis(3),
        WALFileStatus.CONTAINS_SEARCH_INDEX);
    createWalFile(
        timeRetentionDirectory,
        1,
        10,
        now - TimeUnit.HOURS.toMillis(2),
        WALFileStatus.CONTAINS_SEARCH_INDEX);
    createWalFile(
        timeRetentionDirectory,
        2,
        20,
        now - TimeUnit.MINUTES.toMillis(10),
        WALFileStatus.CONTAINS_SEARCH_INDEX);

    final WALNode timeRetentionNode =
        new WALNode("time-retention-node", timeRetentionDirectory, 3, 30);
    try {
      final long cutoffTimeMs = now - TimeUnit.HOURS.toMillis(1);
      assertEquals(20L, timeRetentionNode.getSearchIndexToFreeBeforeTimestamp(cutoffTimeMs));
      assertEquals(2L, timeRetentionNode.getVersionIdToFreeBeforeTimestamp(cutoffTimeMs));

      final long futureCutoffTimeMs = now + TimeUnit.MINUTES.toMillis(1);
      assertEquals(
          Long.MAX_VALUE,
          timeRetentionNode.getSearchIndexToFreeBeforeTimestamp(futureCutoffTimeMs));
      assertEquals(
          Long.MAX_VALUE, timeRetentionNode.getVersionIdToFreeBeforeTimestamp(futureCutoffTimeMs));

      final long earlyCutoffTimeMs = now - TimeUnit.DAYS.toMillis(1);
      assertEquals(
          Long.MIN_VALUE + 1,
          timeRetentionNode.getSearchIndexToFreeBeforeTimestamp(earlyCutoffTimeMs));
      assertEquals(0L, timeRetentionNode.getVersionIdToFreeBeforeTimestamp(earlyCutoffTimeMs));
    } finally {
      timeRetentionNode.close();
      EnvironmentUtils.cleanDir(timeRetentionDirectory);
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
          walNode.log(
              memTableId,
              insertTabletNode,
              Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));
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

  private static void createWalFile(
      final String directory,
      final long versionId,
      final long startSearchIndex,
      final long lastModifiedTime,
      final WALFileStatus status)
      throws IOException {
    assertTrue(new File(directory).mkdirs() || new File(directory).exists());
    final File walFile =
        new File(directory, WALFileUtils.getLogFileName(versionId, startSearchIndex, status));
    assertTrue(walFile.createNewFile());
    assertTrue(walFile.setLastModified(lastModifiedTime));
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
            IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
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
    walNode.rollWALFile();
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());
    executorService.shutdown();
    // recover info from checkpoint file
    Map<Long, MemTableInfo> actualMemTableId2Info =
        CheckpointRecoverUtils.recoverMemTableInfo(new File(logDirectory)).getMemTableId2Info();
    assertEquals(expectedMemTableId2Info, actualMemTableId2Info);
  }

  @Test
  public void testRetentionListenerOnlyRunsWhenBoundaryAdvances() {
    AtomicInteger notifications = new AtomicInteger();
    walNode.setSafeDeletedSearchIndexListener(index -> notifications.incrementAndGet());
    walNode.setSafelyDeletedSearchIndex(0);
    walNode.setSafelyDeletedSearchIndex(0);
    assertEquals(0, notifications.get());
    walNode.setSafelyDeletedSearchIndex(1);
    assertEquals(1, notifications.get());
    walNode.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    assertEquals(2, notifications.get());
    walNode.setSafelyDeletedSearchIndex(0);
    assertEquals(2, notifications.get());
  }

  @Test
  public void testCleanupRechecksLoweredReplicationBoundary() throws Exception {
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    walNode.onMemTableCreated(memTable, tsFilePathInLatestPartition(memTable));
    for (int index = 1; index <= 3; index++) {
      InsertTabletNode node = getInsertTabletNode(devicePath, new long[] {index});
      node.setSearchIndex(index);
      assertEquals(
          WALFlushListener.Status.SUCCESS,
          walNode
              .log(
                  memTable.getMemTableId(),
                  node,
                  Collections.singletonList(new int[] {0, node.getRowCount()}))
              .waitForResult());
      walNode.rollWALFile();
    }
    walNode.onMemTableFlushed(memTable);
    Awaitility.await().until(walNode::isAllWALEntriesConsumed);
    walNode.setSafelyDeletedSearchIndex(Long.MAX_VALUE);
    // Stop a real cleanup between its file-list snapshot and deletion, then register the lower
    // replication boundary. No clock or scheduler timing is involved.
    Class<?> taskClass = Class.forName(WALNode.class.getName() + "$DeleteOutdatedFileTask");
    Constructor<?> constructor = taskClass.getDeclaredConstructor(WALNode.class);
    constructor.setAccessible(true);
    Object task = constructor.newInstance(walNode);
    Method initialize = taskClass.getDeclaredMethod("initAndCheckIfNeedContinue");
    initialize.setAccessible(true);
    assertEquals(true, initialize.invoke(task));
    walNode.setSafelyDeletedSearchIndex(0);
    Method delete = taskClass.getDeclaredMethod("deleteOutdatedFilesAndUpdateMetric");
    delete.setAccessible(true);
    delete.invoke(task);
    int retained = 0;
    for (File file : WALFileUtils.listAllWALFiles(new File(logDirectory))) {
      try (WALReader reader = new WALReader(file)) {
        while (reader.hasNext()) {
          if (reader.next().getValue() instanceof InsertTabletNode) {
            retained++;
          }
        }
      }
    }
    assertEquals(3, retained);
  }

  @Test
  public void testDeleteOutdatedFiles() throws Exception {
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    // write data to make the first WAL file non-empty
    IMemTable memTable = new PrimitiveMemTable(databasePath, dataRegionId);
    long memTableId = memTable.getMemTableId();
    String tsFilePath =
        logDirectory
            + File.separator
            + databasePath
            + File.separator
            + dataRegionId
            + File.separator
            + "-1"
            + File.separator
            + memTableId
            + ".tsfile";
    StorageEngine.getInstance()
        .setDataRegion(
            new DataRegionId(1), new DataRegionTest.DummyDataRegion(logDirectory, databasePath));
    walNode.onMemTableCreated(memTable, tsFilePath);
    for (long time = 1; time <= 100; ++time) {
      InsertTabletNode insertTabletNode =
          getInsertTabletNode(devicePath + memTableId, new long[] {time});
      WALFlushListener walFlushListener =
          walNode.log(
              memTableId,
              insertTabletNode,
              Collections.singletonList(new int[] {0, insertTabletNode.getRowCount()}));
      walFlushListeners.add(walFlushListener);
    }
    walNode.onMemTableFlushed(memTable);
    walNode.rollWALFile();
    walNode.onMemTableCreated(new PrimitiveMemTable(databasePath, dataRegionId), tsFilePath);
    Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());
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
  }

  @Test
  public void testMemTableLargerThanAWalEntryIsFlushedInsteadOfSnapshotted() throws Exception {
    // below the snapshot threshold, but larger than one WAL entry
    RecordingDataRegion dataRegion = cleanUpOldestMemTableOfRamCost(2 * 1024 * 1024);
    assertTrue(dataRegion.flushSubmitted);
    assertFalse(dataRegion.writeLocked);
  }

  @Test
  public void testMemTableWithinAWalEntryIsSnapshotted() throws Exception {
    RecordingDataRegion dataRegion = cleanUpOldestMemTableOfRamCost(512 * 1024);
    assertFalse(dataRegion.flushSubmitted);
    assertTrue(dataRegion.writeLocked);
  }

  /**
   * Lets the WAL cleanup pick an active memTable with the given TVList RAM cost in the latest time
   * partition, with a WAL entry limit of 1 MB (as with the edge profile's wal_buffer_size_in_byte).
   * The WAL is filled by a second memTable, which is flushed, so the effective information ratio is
   * low and the cleanup has to snapshot or flush the oldest one.
   */
  private RecordingDataRegion cleanUpOldestMemTableOfRamCost(long tvListsRamCost) throws Exception {
    int prevEntrySizeLimit = config.getWalEntrySizeLimitInByte();
    config.setWalEntrySizeLimitInByte(1024 * 1024);
    try {
      assertTrue(tvListsRamCost < config.getWalMemTableSnapshotThreshold());
      RecordingDataRegion dataRegion = new RecordingDataRegion(logDirectory, databasePath);
      StorageEngine.getInstance().setDataRegion(new DataRegionId(1), dataRegion);

      IMemTable oldest =
          new PrimitiveMemTable(databasePath, dataRegionId) {
            @Override
            public long getTVListsRamCost() {
              return tvListsRamCost;
            }
          };
      walNode.onMemTableCreated(oldest, tsFilePathInLatestPartition(oldest));
      List<WALFlushListener> listeners = new ArrayList<>();
      for (long time = 1; time <= 10; time++) {
        InsertTabletNode node = getInsertTabletNode(devicePath, new long[] {time});
        listeners.add(
            walNode.log(
                oldest.getMemTableId(),
                node,
                Collections.singletonList(new int[] {0, node.getRowCount()})));
      }

      IMemTable filler = new PrimitiveMemTable(databasePath, dataRegionId);
      walNode.onMemTableCreated(filler, tsFilePathInLatestPartition(filler));
      for (long time = 1; time <= 20000; time++) {
        InsertTabletNode node = getInsertTabletNode(devicePath, new long[] {time});
        listeners.add(
            walNode.log(
                filler.getMemTableId(),
                node,
                Collections.singletonList(new int[] {0, node.getRowCount()})));
      }
      walNode.onMemTableFlushed(filler);
      Awaitility.await().until(() -> walNode.isAllWALEntriesConsumed());
      for (WALFlushListener listener : listeners) {
        assertEquals(WALFlushListener.Status.SUCCESS, listener.waitForResult());
      }

      walNode.deleteOutdatedFiles();
      return dataRegion;
    } finally {
      config.setWalEntrySizeLimitInByte(prevEntrySizeLimit);
    }
  }

  private static String tsFilePathInLatestPartition(IMemTable memTable) {
    return logDirectory
        + File.separator
        + databasePath
        + File.separator
        + dataRegionId
        + File.separator
        + "0"
        + File.separator
        + memTable.getMemTableId()
        + ".tsfile";
  }

  /** Records whether the WAL cleanup flushed a memTable or took the write lock to snapshot it. */
  private static class RecordingDataRegion extends DataRegionTest.DummyDataRegion {
    private volatile boolean flushSubmitted;
    private volatile boolean writeLocked;

    private RecordingDataRegion(String systemInfoDir, String databaseName)
        throws DataRegionException {
      super(systemInfoDir, databaseName);
    }

    @Override
    public boolean submitAFlushTask(long timeRangeId, boolean sequence, IMemTable memTable) {
      flushSubmitted = true;
      return false;
    }

    @Override
    public void writeLock(String holder) {
      writeLocked = true;
      super.writeLock(holder);
    }
  }
}
