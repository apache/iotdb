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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryModificationLoaderTest {

  private static final IDeviceID DEVICE_ID = new PlainDeviceID("root.sg.d1");

  private File testDir;

  @After
  public void tearDown() throws Exception {
    if (testDir != null && testDir.exists()) {
      FileUtils.deleteDirectory(testDir);
    }
  }

  @Test
  public void testCacheLoadedModsTreeWhenQuotaEnough() throws Exception {
    TsFileResource resource = prepareResource("cache");
    writeMods(resource, deletion("root.sg.d1.s1", 0, 10), deletion("root.sg.d2.s1", 20, 30));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager();

    try (QueryModificationLoader loader =
        newLoader(
            resource,
            Long.MAX_VALUE,
            fileModCache,
            cachedModEntriesSize,
            memoryReservationManager,
            1)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(1, result.size());
      assertTrue(fileModCache.containsKey(resource.getTsFileID()));
      assertTrue(cachedModEntriesSize.get() > 0);
      assertTrue(memoryReservationManager.getReservedBytes() >= cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getImmediateReservationCount() > 0);
    }
  }

  @Test
  public void testFallbackScansModsWhenFileSizeExceedsRemainingQuotaBeforeLoad() throws Exception {
    TsFileResource resource = prepareResource("file-size-precheck-fallback");
    writeMods(
        resource,
        deletion("root.sg.d1.s1", 0, 10),
        deletion("root.sg.d2.s1", 20, 30),
        deletion("root.sg.d1.s1", 40, 50));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager();

    try (QueryModificationLoader loader =
        newLoader(resource, 1, fileModCache, cachedModEntriesSize, memoryReservationManager, 1)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(2, result.size());
      assertFalse(fileModCache.containsKey(resource.getTsFileID()));
      assertEquals(0, cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getReservedBytes() > 0);
      assertEquals(0, memoryReservationManager.getRemainingImmediateFailures());
      assertEquals(0, memoryReservationManager.getImmediateReservationCount());
    }
  }

  @Test
  public void testFallbackScansRemainingModsWhenEstimatedTreeExceedsQuota() throws Exception {
    TsFileResource resource = prepareResource("estimated-tree-quota-fallback");
    writeMods(
        resource,
        deletion("root.sg.d1.s1", 0, 10),
        deletion("root.sg.d2.s1", 20, 30),
        deletion("root.sg.d1.s1", 40, 50));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager();

    try (QueryModificationLoader loader =
        newLoader(
            resource,
            resource.getModFile().getSize() + 1,
            fileModCache,
            cachedModEntriesSize,
            memoryReservationManager,
            1)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(2, result.size());
      assertFalse(fileModCache.containsKey(resource.getTsFileID()));
      assertEquals(0, cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getReservedBytes() > 0);
      assertTrue(memoryReservationManager.getCumulativeReleaseCount() > 0);
    }
  }

  @Test
  public void testFallbackMatchesLoadedTreeWhenFinalReservationFailed() throws Exception {
    TsFileResource resource = prepareResource("final-reserve-failed");
    writeMods(
        resource,
        deletion("root.sg.d1.s1", 0, 10),
        deletion("root.sg.d2.s1", 20, 30),
        deletion("root.sg.d1.s1", 40, 50));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager(1);

    try (QueryModificationLoader loader =
        newLoader(
            resource,
            Long.MAX_VALUE,
            fileModCache,
            cachedModEntriesSize,
            memoryReservationManager,
            100)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(2, result.size());
      assertFalse(fileModCache.containsKey(resource.getTsFileID()));
      assertEquals(0, cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getReservedBytes() > 0);
      assertEquals(0, memoryReservationManager.getRemainingImmediateFailures());
      assertEquals(1, memoryReservationManager.getImmediateReservationCount());
    }
  }

  @Test
  public void testFallbackReservesMatchedModsCumulativelyWhenQuotaExceeded() throws Exception {
    TsFileResource resource = prepareResource("fallback-cumulative-reserve");
    writeMods(
        resource,
        deletion("root.sg.d1.s1", 0, 10),
        deletion("root.sg.d2.s1", 20, 30),
        deletion("root.sg.d1.s1", 40, 50));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager(1);

    try (QueryModificationLoader loader =
        newLoader(resource, 1, fileModCache, cachedModEntriesSize, memoryReservationManager, 1)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(2, result.size());
      assertFalse(fileModCache.containsKey(resource.getTsFileID()));
      assertEquals(0, cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getReservedBytes() > 0);
      assertEquals(1, memoryReservationManager.getRemainingImmediateFailures());
      assertEquals(0, memoryReservationManager.getImmediateReservationCount());
    }
  }

  @Test
  public void testFallbackAdjustsReservedMemoryAfterSortAndMerge() throws Exception {
    TsFileResource resource = prepareResource("fallback-sort-merge-adjust");
    writeMods(
        resource,
        deletion("root.sg.d1.s1", 0, 10),
        deletion("root.sg.d1.s1", 5, 15),
        deletion("root.sg.d2.s1", 20, 30));

    Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>> fileModCache =
        new ConcurrentHashMap<>();
    AtomicLong cachedModEntriesSize = new AtomicLong();
    CountingMemoryReservationManager memoryReservationManager =
        new CountingMemoryReservationManager();

    try (QueryModificationLoader loader =
        newLoader(resource, 1, fileModCache, cachedModEntriesSize, memoryReservationManager, 1)) {
      List<Modification> result = loader.getPathModifications();

      assertEquals(1, result.size());
      assertFalse(fileModCache.containsKey(resource.getTsFileID()));
      assertEquals(0, cachedModEntriesSize.get());
      assertTrue(memoryReservationManager.getReservedBytes() > 0);
      assertTrue(memoryReservationManager.getCumulativeReleaseCount() > 0);
    }
  }

  private QueryModificationLoader newLoader(
      TsFileResource resource,
      long modsCacheSizeLimitPerFI,
      Map<TsFileID, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
          fileModCache,
      AtomicLong cachedModEntriesSize,
      MemoryReservationManager memoryReservationManager,
      int modsMemoryEstimateReadInterval)
      throws IllegalPathException {
    QueryContext queryContext = new QueryContext(false);
    PartialPath queryPath = new PartialPath(DEVICE_ID, "s1");
    return new QueryModificationLoader(
        resource,
        memoryReservationManager,
        modsCacheSizeLimitPerFI,
        modsMemoryEstimateReadInterval,
        fileModCache,
        cachedModEntriesSize,
        modification -> modification.getPath().overlapWith(queryPath),
        modsTree -> queryContext.getPathModifications(modsTree, queryPath));
  }

  private TsFileResource prepareResource(String name) {
    testDir = new File(TestConstant.BASE_OUTPUT_PATH, "QueryModificationLoaderTest-" + name);
    testDir.mkdirs();
    File tsFile =
        new File(TsFileNameGenerator.generateNewTsFilePath(testDir.getAbsolutePath(), 1, 1, 0, 0));
    return new TsFileResource(tsFile);
  }

  private static Deletion deletion(String path, long startTime, long endTime)
      throws IllegalPathException {
    return new Deletion(new MeasurementPath(path), 0, startTime, endTime);
  }

  private void writeMods(TsFileResource resource, Deletion... modifications) throws Exception {
    try (ModificationFile modificationFile = resource.getModFile()) {
      for (Deletion modification : modifications) {
        modificationFile.write(modification);
      }
    }
  }

  private static class CountingMemoryReservationManager implements MemoryReservationManager {

    private long reservedBytes;
    private int remainingImmediateFailures;
    private int immediateReservationCount;
    private int cumulativeReleaseCount;

    private CountingMemoryReservationManager() {}

    private CountingMemoryReservationManager(int remainingImmediateFailures) {
      this.remainingImmediateFailures = remainingImmediateFailures;
    }

    @Override
    public void reserveMemoryCumulatively(long size) {
      reservedBytes += size;
    }

    @Override
    public void reserveMemoryImmediately() {
      immediateReservationCount++;
      if (remainingImmediateFailures > 0) {
        remainingImmediateFailures--;
        throw new MemoryNotEnoughException("Mock memory reservation failure.");
      }
    }

    @Override
    public void reserveMemoryImmediately(long size) {
      immediateReservationCount++;
      if (remainingImmediateFailures > 0) {
        remainingImmediateFailures--;
        throw new MemoryNotEnoughException("Mock memory reservation failure.");
      }
      reservedBytes += size;
    }

    @Override
    public void releaseMemoryCumulatively(long size) {
      cumulativeReleaseCount++;
      reservedBytes -= size;
    }

    @Override
    public void releaseMemoryImmediately(long size) {
      reservedBytes -= size;
    }

    @Override
    public void releaseAllReservedMemory() {
      reservedBytes = 0;
    }

    @Override
    public Pair<Long, Long> releaseMemoryVirtually(long size) {
      reservedBytes -= size;
      return new Pair<>(size, 0L);
    }

    @Override
    public void reserveMemoryVirtually(long bytesToBeReserved, long bytesAlreadyReserved) {
      reservedBytes += bytesToBeReserved + bytesAlreadyReserved;
    }

    @Override
    public void setHighestPriority(boolean isHighestPriority) {}

    private long getReservedBytes() {
      return reservedBytes;
    }

    private int getRemainingImmediateFailures() {
      return remainingImmediateFailures;
    }

    private int getImmediateReservationCount() {
      return immediateReservationCount;
    }

    private int getCumulativeReleaseCount() {
      return cumulativeReleaseCount;
    }
  }
}
