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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DataRegionConsistencyManagerTest {

  private final List<Path> tempStateDirs = new ArrayList<>();

  @After
  public void tearDown() throws IOException {
    for (Path tempStateDir : tempStateDirs) {
      deleteRecursively(tempStateDir);
    }
    tempStateDirs.clear();
  }

  @Test
  public void compactionShouldNotAdvancePartitionMutationEpoch() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 901);

    consistencyManager.onPartitionMutation(consensusGroupId, 7L);
    long beforeEpoch = getPartitionMutationEpoch(consistencyManager, consensusGroupId, 7L);

    consistencyManager.onCompaction(
        consensusGroupId,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        7L);

    Assert.assertEquals(
        beforeEpoch, getPartitionMutationEpoch(consistencyManager, consensusGroupId, 7L));
    Assert.assertEquals(
        RepairProgressTable.SnapshotState.DIRTY,
        getSnapshotState(consistencyManager, consensusGroupId, 7L));
  }

  @Test
  public void deletionShouldMarkAffectedPartitionDirty() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 902);

    consistencyManager.onDeletion(consensusGroupId, 0L, 0L);

    Assert.assertEquals(1L, getPartitionMutationEpoch(consistencyManager, consensusGroupId, 0L));
    Assert.assertEquals(
        RepairProgressTable.SnapshotState.DIRTY,
        getSnapshotState(consistencyManager, consensusGroupId, 0L));
  }

  @Test
  public void logicalRepairMutationShouldNotAdvancePartitionMutationEpoch() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 903);

    consistencyManager.onPartitionMutation(consensusGroupId, 11L);
    long beforeEpoch = getPartitionMutationEpoch(consistencyManager, consensusGroupId, 11L);

    consistencyManager.runWithLogicalRepairMutation(
        consensusGroupId,
        11L,
        "1:11:0:" + beforeEpoch + ":" + beforeEpoch,
        () -> {
          consistencyManager.onPartitionMutation(consensusGroupId, 11L);
          return null;
        });

    Assert.assertEquals(
        beforeEpoch, getPartitionMutationEpoch(consistencyManager, consensusGroupId, 11L));
    Assert.assertEquals(
        RepairProgressTable.SnapshotState.DIRTY,
        getSnapshotState(consistencyManager, consensusGroupId, 11L));
  }

  @Test
  public void exactRepairSelectorShouldRoundTripExactKeys() throws Exception {
    Class<?> selectorClass =
        Class.forName(
            "org.apache.iotdb.db.storageengine.dataregion.consistency.DataRegionConsistencyManager$LogicalLeafSelector");
    Method parseMethod = selectorClass.getDeclaredMethod("parse", String.class);
    parseMethod.setAccessible(true);

    String selectorToken =
        "leaf:4:9@@@"
            + Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                    String.join("\n", "root.db.d1|s1|1|INT64|1", "root.db.d1|s2|2|INT64|2")
                        .getBytes(StandardCharsets.UTF_8));
    Object selector = parseMethod.invoke(null, selectorToken);

    Field exactKeysField = selectorClass.getDeclaredField("exactKeys");
    exactKeysField.setAccessible(true);

    @SuppressWarnings("unchecked")
    java.util.Set<String> exactKeys = (java.util.Set<String>) exactKeysField.get(selector);
    Assert.assertEquals(2, exactKeys.size());
    Assert.assertTrue(exactKeys.contains("root.db.d1|s1|1|INT64|1"));
    Assert.assertTrue(exactKeys.contains("root.db.d1|s2|2|INT64|2"));
  }

  @Test
  public void rangeRepairSelectorShouldHonorLogicalKeyRange() throws Exception {
    Class<?> selectorClass =
        Class.forName(
            "org.apache.iotdb.db.storageengine.dataregion.consistency.DataRegionConsistencyManager$LogicalLeafSelector");
    Method parseMethod = selectorClass.getDeclaredMethod("parse", String.class);
    parseMethod.setAccessible(true);

    Method computeDeviceShardMethod =
        DataRegionConsistencyManager.class.getDeclaredMethod("computeDeviceShard", String.class);
    computeDeviceShardMethod.setAccessible(true);
    int shard = (Integer) computeDeviceShardMethod.invoke(null, "root.db.d1");

    String selectorToken =
        "leaf:"
            + shard
            + ":0@"
            + encodeBase64("root.db.d1|s1|1|INT64|1")
            + "@"
            + encodeBase64("root.db.d1|s1|3|INT64|3")
            + "@";
    Object selector = parseMethod.invoke(null, selectorToken);

    Method matchesLiveCellMethod =
        selectorClass.getDeclaredMethod(
            "matchesLiveCell",
            String.class,
            String.class,
            TSDataType.class,
            long.class,
            Object.class);
    matchesLiveCellMethod.setAccessible(true);
    Method requiresScopedResetMethod = selectorClass.getDeclaredMethod("requiresScopedReset");
    requiresScopedResetMethod.setAccessible(true);

    Assert.assertTrue((Boolean) requiresScopedResetMethod.invoke(selector));
    Assert.assertTrue(
        (Boolean)
            matchesLiveCellMethod.invoke(selector, "root.db.d1", "s1", TSDataType.INT64, 2L, 2L));
    Assert.assertFalse(
        (Boolean)
            matchesLiveCellMethod.invoke(selector, "root.db.d1", "s1", TSDataType.INT64, 4L, 4L));
    Assert.assertFalse(
        (Boolean)
            matchesLiveCellMethod.invoke(selector, "root.db.d1", "s1", TSDataType.INT64, 0L, 0L));
  }

  @Test
  public void snapshotTreeShouldTrackLogicalKeyBounds() throws Exception {
    Class<?> snapshotTreeClass =
        Class.forName(
            "org.apache.iotdb.db.storageengine.dataregion.consistency.DataRegionConsistencyManager$SnapshotTree");
    Method emptyMethod = snapshotTreeClass.getDeclaredMethod("empty");
    emptyMethod.setAccessible(true);
    Object snapshotTree = emptyMethod.invoke(null);

    Method addLeafEntryMethod =
        snapshotTreeClass.getDeclaredMethod(
            "addLeafEntry", String.class, int.class, String.class, long.class, long.class);
    addLeafEntryMethod.setAccessible(true);
    addLeafEntryMethod.invoke(snapshotTree, "leaf:7:0", 7, "root.db.d1|s1|2|INT64|2", 11L, 1L);
    addLeafEntryMethod.invoke(snapshotTree, "leaf:7:0", 7, "root.db.d1|s1|1|INT64|1", 13L, 1L);
    addLeafEntryMethod.invoke(snapshotTree, "leaf:7:0", 7, "root.db.d1|s1|3|INT64|3", 17L, 1L);

    Field nodesByHandleField = snapshotTreeClass.getDeclaredField("nodesByHandle");
    nodesByHandleField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> nodesByHandle = (Map<String, Object>) nodesByHandleField.get(snapshotTree);
    Object leafNode = nodesByHandle.get("leaf:7:0");
    Assert.assertNotNull(leafNode);

    Field keyRangeStartField = leafNode.getClass().getDeclaredField("keyRangeStart");
    keyRangeStartField.setAccessible(true);
    Field keyRangeEndField = leafNode.getClass().getDeclaredField("keyRangeEnd");
    keyRangeEndField.setAccessible(true);

    Assert.assertEquals("root.db.d1|s1|1|INT64|1", keyRangeStartField.get(leafNode));
    Assert.assertEquals("root.db.d1|s1|3|INT64|3", keyRangeEndField.get(leafNode));
  }

  @Test
  public void memTableSeriesDiscoveryShouldIncludeUnsealedMeasurements() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();

    IMeasurementSchema alignedS1 = new MeasurementSchema("s1", TSDataType.INT64);
    IMeasurementSchema alignedS2 = new MeasurementSchema("s2", TSDataType.DOUBLE);
    List<IMeasurementSchema> alignedSchemas = java.util.Arrays.asList(alignedS1, alignedS2);
    AlignedWritableMemChunkGroup alignedGroup =
        new AlignedWritableMemChunkGroup(alignedSchemas, false);
    alignedGroup.writeRow(1L, new Object[] {1L, 1.0d}, alignedSchemas);

    IMeasurementSchema nonAlignedSchema = new MeasurementSchema("s3", TSDataType.INT32);
    WritableMemChunkGroup nonAlignedGroup = new WritableMemChunkGroup();
    nonAlignedGroup.writeRow(2L, new Object[] {1}, Collections.singletonList(nonAlignedSchema));

    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = new HashMap<>();
    memTableMap.put(IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1"), alignedGroup);
    memTableMap.put(IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d2"), nonAlignedGroup);
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.db", "1", memTableMap);

    Method collectMemTableSeriesContexts =
        DataRegionConsistencyManager.class.getDeclaredMethod(
            "collectMemTableSeriesContexts", IMemTable.class, Map.class);
    collectMemTableSeriesContexts.setAccessible(true);

    Map<String, Object> deviceSeriesContexts = new java.util.TreeMap<>();
    collectMemTableSeriesContexts.invoke(consistencyManager, memTable, deviceSeriesContexts);

    Assert.assertEquals(2, deviceSeriesContexts.size());
    assertDeviceSeriesContext(deviceSeriesContexts.get("root.db.d1"), true, "s1", "s2");
    assertDeviceSeriesContext(deviceSeriesContexts.get("root.db.d2"), false, "s3");
  }

  @Test
  public void closedChannelFailuresShouldBeClassifiedAsRetryable() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    Method method =
        DataRegionConsistencyManager.class.getDeclaredMethod(
            "isRetryableSnapshotReadFailure", Throwable.class);
    method.setAccessible(true);

    Assert.assertTrue((Boolean) method.invoke(consistencyManager, new ClosedChannelException()));
    Assert.assertTrue(
        (Boolean)
            method.invoke(
                consistencyManager, new IOException("wrapped", new ClosedChannelException())));
    Assert.assertFalse(
        (Boolean) method.invoke(consistencyManager, new IOException("other io failure")));
  }

  @Test
  public void inspectPartitionShouldRebuildSnapshotEvenWhenWorkingProcessorsExist()
      throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 904);
    DataRegion dataRegion = createDataRegionWithEmptySnapshotInputs(1L, true);

    DataRegionConsistencyManager.PartitionInspection inspection =
        consistencyManager.inspectPartition(
            consensusGroupId, dataRegion, 1L, Collections.emptyList());

    Assert.assertEquals(1L, inspection.getPartitionId());
    Assert.assertEquals(RepairProgressTable.SnapshotState.READY, inspection.getSnapshotState());
    Assert.assertEquals(0L, inspection.getPartitionMutationEpoch());
    Assert.assertEquals(0L, inspection.getSnapshotEpoch());
  }

  @Test
  public void knownPartitionsShouldRecoverFromPersistedPartitionState() throws Exception {
    Path stateDir = Files.createTempDirectory("logical-consistency-partition-state");
    try {
      LogicalConsistencyPartitionStateStore store =
          new LogicalConsistencyPartitionStateStore(stateDir);
      DataRegionConsistencyManager writer = new DataRegionConsistencyManager(store);
      TConsensusGroupId consensusGroupId =
          new TConsensusGroupId(TConsensusGroupType.DataRegion, 905);

      writer.onPartitionMutation(consensusGroupId, 2L);

      DataRegionConsistencyManager recovered = new DataRegionConsistencyManager(store);
      Assert.assertEquals(
          Collections.singletonList(2L), recovered.getKnownPartitions(consensusGroupId));
      Assert.assertEquals(1L, getPartitionMutationEpoch(recovered, consensusGroupId, 2L));
    } finally {
      deleteRecursively(stateDir);
    }
  }

  @Test
  public void persistKnownPartitionsShouldNotBlockConcurrentMutation() throws Exception {
    BlockingPartitionStateStore store = new BlockingPartitionStateStore();
    DataRegionConsistencyManager consistencyManager = new DataRegionConsistencyManager(store);
    TConsensusGroupId consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 906);
    DataRegion dataRegion = createDataRegionWithEmptySnapshotInputs(1L, true);
    AtomicReference<Throwable> inspectionFailure = new AtomicReference<>();

    Thread inspectionThread =
        new Thread(
            () -> {
              try {
                consistencyManager.inspectPartition(
                    consensusGroupId, dataRegion, 1L, Collections.emptyList());
              } catch (Throwable t) {
                inspectionFailure.set(t);
              }
            });
    inspectionThread.start();

    Assert.assertTrue(
        "The initial persistence should start", store.awaitPersistStarted(5, TimeUnit.SECONDS));

    long mutationStartNanos = System.nanoTime();
    consistencyManager.onPartitionMutation(consensusGroupId, 2L);
    long mutationElapsedMillis =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - mutationStartNanos);
    Assert.assertTrue(
        "Concurrent partition mutation should not wait on slow persistence",
        mutationElapsedMillis < 1_000L);

    store.releasePersist();
    inspectionThread.join(TimeUnit.SECONDS.toMillis(5));
    if (inspectionFailure.get() != null) {
      throw new AssertionError("Inspection thread should succeed", inspectionFailure.get());
    }

    Map<Long, Long> persistedState = store.load(consensusGroupId.toString());
    Assert.assertEquals(Long.valueOf(0L), persistedState.get(1L));
    Assert.assertEquals(Long.valueOf(1L), persistedState.get(2L));
  }

  @Test
  public void collectLogicalSeriesContextsShouldNotPoisonSharedReaders() throws Exception {
    DataRegionConsistencyManager consistencyManager = newTestConsistencyManager();
    Path tempDir = Files.createTempDirectory("consistency-manager-reader-regression");
    TsFileResource resource = null;
    MultiTsFileDeviceIterator verificationIterator = null;
    try {
      Path tsFileDir = tempDir.resolve("sequence/root.testsg/1/1");
      Files.createDirectories(tsFileDir);
      File tsFile = tsFileDir.resolve("1-1-0-0.tsfile").toFile();
      resource = new TsFileResource(tsFile);
      try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
        writer.startChunkGroup("d1");
        writer.generateSimpleNonAlignedSeriesToCurrentDevice(
            "s1", new TimeRange[] {new TimeRange(1, 3)}, TSEncoding.PLAIN, CompressionType.LZ4);
        writer.endChunkGroup();
        writer.endFile();
      }
      resource.close();

      TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
      DataRegion dataRegion = Mockito.mock(DataRegion.class);
      Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
      Mockito.when(tsFileManager.getTsFileListSnapshot(1L, true))
          .thenReturn(Collections.singletonList(resource));
      Mockito.when(tsFileManager.getTsFileListSnapshot(1L, false))
          .thenReturn(Collections.emptyList());
      Mockito.when(dataRegion.getWorkSequenceTsFileProcessors())
          .thenReturn(Collections.emptyList());
      Mockito.when(dataRegion.getWorkUnsequenceTsFileProcessors())
          .thenReturn(Collections.emptyList());

      Method method =
          DataRegionConsistencyManager.class.getDeclaredMethod(
              "collectLogicalSeriesContexts", DataRegion.class, long.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<String, Object> deviceSeriesContexts =
          (Map<String, Object>) method.invoke(consistencyManager, dataRegion, 1L);
      assertDeviceSeriesContext(deviceSeriesContexts.get("root.testsg.d1"), false, "s1");

      verificationIterator =
          new MultiTsFileDeviceIterator(
              Collections.singletonList(resource), Collections.emptyList());
      Assert.assertTrue(verificationIterator.hasNextDevice());
      Assert.assertEquals("root.testsg.d1", String.valueOf(verificationIterator.nextDevice().left));
      Assert.assertTrue(verificationIterator.getAllSchemasOfCurrentDevice().containsKey("s1"));
    } finally {
      if (resource != null) {
        FileReaderManager.getInstance().closeFileAndRemoveReader(resource.getTsFileID());
      }
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  private String encodeBase64(String value) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private DataRegion createDataRegionWithEmptySnapshotInputs(
      long partitionId, boolean includeWorkingProcessor) throws Exception {
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    Mockito.when(tsFileManager.getTsFileListSnapshot(partitionId, true))
        .thenReturn(Collections.emptyList());
    Mockito.when(tsFileManager.getTsFileListSnapshot(partitionId, false))
        .thenReturn(Collections.emptyList());
    Mockito.when(dataRegion.getWorkUnsequenceTsFileProcessors())
        .thenReturn(Collections.emptyList());

    if (!includeWorkingProcessor) {
      Mockito.when(dataRegion.getWorkSequenceTsFileProcessors())
          .thenReturn(Collections.emptyList());
      return dataRegion;
    }

    TsFileProcessor processor = Mockito.mock(TsFileProcessor.class);
    IMemTable emptyMemTable = Mockito.mock(IMemTable.class);
    Mockito.when(processor.getTimeRangeId()).thenReturn(partitionId);
    Mockito.when(processor.tryReadLock(1_000L)).thenReturn(true);
    Mockito.when(processor.getWorkMemTable()).thenReturn(emptyMemTable);
    Mockito.when(processor.getFlushingMemTable()).thenReturn(new ConcurrentLinkedDeque<>());
    Mockito.doNothing().when(processor).readUnLock();
    Mockito.when(emptyMemTable.getMemTableMap()).thenReturn(Collections.emptyMap());
    Mockito.when(dataRegion.getWorkSequenceTsFileProcessors())
        .thenReturn(Collections.singletonList(processor));
    return dataRegion;
  }

  private DataRegionConsistencyManager newTestConsistencyManager() throws IOException {
    Path tempStateDir = Files.createTempDirectory("logical-consistency-test-state");
    tempStateDirs.add(tempStateDir);
    return new DataRegionConsistencyManager(
        new LogicalConsistencyPartitionStateStore(tempStateDir));
  }

  private void deleteRecursively(Path root) throws IOException {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try {
      Files.walk(root)
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @SuppressWarnings("unchecked")
  private void assertDeviceSeriesContext(
      Object deviceSeriesContext, boolean expectedAligned, String... expectedMeasurements)
      throws Exception {
    Assert.assertNotNull(deviceSeriesContext);

    Field alignedField = deviceSeriesContext.getClass().getDeclaredField("aligned");
    alignedField.setAccessible(true);
    Assert.assertEquals(expectedAligned, alignedField.getBoolean(deviceSeriesContext));

    Field measurementSchemasField =
        deviceSeriesContext.getClass().getDeclaredField("measurementSchemas");
    measurementSchemasField.setAccessible(true);
    Map<String, Object> measurementSchemas =
        (Map<String, Object>) measurementSchemasField.get(deviceSeriesContext);
    Assert.assertEquals(expectedMeasurements.length, measurementSchemas.size());
    for (String expectedMeasurement : expectedMeasurements) {
      Assert.assertTrue(measurementSchemas.containsKey(expectedMeasurement));
    }
  }

  @SuppressWarnings("unchecked")
  private Object getPartitionState(
      DataRegionConsistencyManager consistencyManager,
      TConsensusGroupId consensusGroupId,
      long partitionId)
      throws Exception {
    Field regionStatesField = DataRegionConsistencyManager.class.getDeclaredField("regionStates");
    regionStatesField.setAccessible(true);
    ConcurrentHashMap<String, Object> regionStates =
        (ConcurrentHashMap<String, Object>) regionStatesField.get(consistencyManager);
    Object regionState = regionStates.get(consensusGroupId.toString());
    Assert.assertNotNull(regionState);

    Field partitionsField = regionState.getClass().getDeclaredField("partitions");
    partitionsField.setAccessible(true);
    Map<Long, Object> partitions = (Map<Long, Object>) partitionsField.get(regionState);
    Object partitionState = partitions.get(partitionId);
    Assert.assertNotNull(partitionState);
    return partitionState;
  }

  private long getPartitionMutationEpoch(
      DataRegionConsistencyManager consistencyManager,
      TConsensusGroupId consensusGroupId,
      long partitionId)
      throws Exception {
    Object partitionState = getPartitionState(consistencyManager, consensusGroupId, partitionId);
    Field mutationEpochField = partitionState.getClass().getDeclaredField("partitionMutationEpoch");
    mutationEpochField.setAccessible(true);
    return mutationEpochField.getLong(partitionState);
  }

  private RepairProgressTable.SnapshotState getSnapshotState(
      DataRegionConsistencyManager consistencyManager,
      TConsensusGroupId consensusGroupId,
      long partitionId)
      throws Exception {
    Object partitionState = getPartitionState(consistencyManager, consensusGroupId, partitionId);
    Field snapshotStateField = partitionState.getClass().getDeclaredField("snapshotState");
    snapshotStateField.setAccessible(true);
    return (RepairProgressTable.SnapshotState) snapshotStateField.get(partitionState);
  }

  private static class BlockingPartitionStateStore extends LogicalConsistencyPartitionStateStore {
    private final CountDownLatch persistStarted = new CountDownLatch(1);
    private final CountDownLatch allowPersist = new CountDownLatch(1);
    private final ConcurrentHashMap<String, Map<Long, Long>> persisted = new ConcurrentHashMap<>();

    @Override
    public void persist(String consensusGroupKey, Map<Long, Long> mutationEpochs)
        throws IOException {
      persistStarted.countDown();
      try {
        allowPersist.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while blocking persist", e);
      }
      persisted.put(consensusGroupKey, new HashMap<>(mutationEpochs));
    }

    @Override
    public Map<Long, Long> load(String consensusGroupKey) {
      return persisted.getOrDefault(consensusGroupKey, Collections.emptyMap());
    }

    private boolean awaitPersistStarted(long timeout, TimeUnit unit) throws InterruptedException {
      return persistStarted.await(timeout, unit);
    }

    private void releasePersist() {
      allowPersist.countDown();
    }
  }
}
