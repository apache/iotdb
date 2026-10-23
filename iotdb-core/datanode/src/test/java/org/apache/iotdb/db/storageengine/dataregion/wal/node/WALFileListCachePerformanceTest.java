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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.TreeMap;

public class WALFileListCachePerformanceTest {

  private static final String ENABLED_PROPERTY = "iotdb.wal.file-list-cache.perf.enabled";
  private static final String FILE_COUNTS_PROPERTY = "iotdb.wal.file-list-cache.perf.file-counts";
  private static final String WARMUP_READS_PROPERTY = "iotdb.wal.file-list-cache.perf.warmup-reads";
  private static final String READS_PROPERTY = "iotdb.wal.file-list-cache.perf.reads";
  private static final String ROUNDS_PROPERTY = "iotdb.wal.file-list-cache.perf.rounds";

  private static final String BASE_DIRECTORY =
      TestConstant.BASE_OUTPUT_PATH.concat("wal-file-list-cache-performance");
  private static final int[] DEFAULT_FILE_COUNTS = {1, 10, 100, 1000};

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final long TREE_MAP_ENTRY_SHALLOW_SIZE = getTreeMapEntryShallowSize();
  private static final long FILE_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(File.class);
  private static final long LONG_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Long.class);
  private static final long STRING_SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(String.class);
  private static final boolean COMPACT_STRINGS_ENABLED = isCompactStringsEnabled();

  private static volatile long benchmarkBlackhole;

  /**
   * Compares repeated sorted-WAL-file reads with the cache disabled and enabled for increasing
   * directory sizes. The cache-disabled path must rescan and sort on every read, while the enabled
   * path should reuse the immutable snapshot after warmup.
   */
  @Test
  public void benchmarkRepeatedSortedWalFileReads() throws Exception {
    assumePerformanceTestEnabled();

    final int[] fileCounts = parseFileCounts();
    final int warmupReads = Integer.getInteger(WARMUP_READS_PROPERTY, 500);
    final int reads = Integer.getInteger(READS_PROPERTY, 2000);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, 5);
    Assert.assertTrue(warmupReads > 0);
    Assert.assertTrue(reads > 0);
    Assert.assertTrue(rounds > 0);

    final boolean originalCacheEnabled = CONFIG.isWalFileListCacheEnabled();
    EnvironmentUtils.cleanDir(BASE_DIRECTORY);
    try {
      for (int fileCount : fileCounts) {
        runScenario(fileCount, warmupReads, reads, rounds);
      }
    } finally {
      CONFIG.setWalFileListCacheEnabled(originalCacheEnabled);
      EnvironmentUtils.cleanDir(BASE_DIRECTORY);
    }
  }

  /**
   * Estimates the retained heap added by enabling the cache for increasing WAL directory sizes. The
   * estimate uses the current JVM object layout and the actual cached file paths, and separates the
   * incrementally maintained index from the snapshot that is retained only after first access.
   */
  @Test
  public void measureRetainedWalFileListCacheMemory() throws Exception {
    assumePerformanceTestEnabled();

    final boolean originalCacheEnabled = CONFIG.isWalFileListCacheEnabled();
    EnvironmentUtils.cleanDir(BASE_DIRECTORY);
    try {
      for (int fileCount : parseFileCounts()) {
        runMemoryScenario(fileCount);
      }
    } finally {
      CONFIG.setWalFileListCacheEnabled(originalCacheEnabled);
      EnvironmentUtils.cleanDir(BASE_DIRECTORY);
    }
  }

  private static void runMemoryScenario(int fileCount) throws Exception {
    Assert.assertTrue(fileCount > 0);
    final WALNode cacheDisabled = createWalNode(fileCount, false);
    final WALNode cacheEnabled = createWalNode(fileCount, true);
    try {
      Assert.assertNull(cacheDisabled.getCachedSortedWalFiles());
      Assert.assertNull(cacheEnabled.getCachedSortedWalFiles());
      assertFileList(cacheDisabled.getSortedWalFilesForTest(), fileCount);
      assertFileList(cacheEnabled.getSortedWalFilesForTest(), fileCount);
      Assert.assertNull(cacheDisabled.getCachedSortedWalFiles());

      final File[] cachedWalFiles = cacheEnabled.getCachedSortedWalFiles();
      Assert.assertNotNull(cachedWalFiles);
      assertFileList(cachedWalFiles, fileCount);

      final long entryBytes = TREE_MAP_ENTRY_SHALLOW_SIZE * cachedWalFiles.length;
      final long fileBytes = FILE_SHALLOW_SIZE * cachedWalFiles.length;
      long keyBytes = 0;
      long pathBytes = 0;
      long pathCharacters = 0;
      for (File walFile : cachedWalFiles) {
        pathBytes += estimateStringBytes(walFile.getPath());
        pathCharacters += walFile.getPath().length();
        final long versionId = WALFileUtils.parseVersionId(walFile.getName());
        // Long values in this range come from the JVM-wide cache and are not retained by this
        // feature.
        if (versionId < -128 || versionId > 127) {
          keyBytes += LONG_SHALLOW_SIZE;
        }
      }
      final long indexBytes = entryBytes + fileBytes + keyBytes + pathBytes;
      // TreeMap.values() is only a temporary view while publishing the array snapshot.
      final long snapshotBytes = RamUsageEstimator.sizeOfObjectArray(cachedWalFiles.length);
      final long totalBytes = indexBytes + snapshotBytes;
      System.out.printf(
          Locale.ROOT,
          "WAL file-list cache retained-memory estimate: files=%d%n"
              + "  cache=false retained=0 B%n"
              + "  cache=true  index=%d B (%.3f KiB), lazy-snapshot=%d B (%.3f KiB), "
              + "total=%d B (%.3f KiB), total/file=%.2f B%n"
              + "  breakdown   entries=%d B, keys=%d B, files=%d B, paths=%d B, "
              + "avg-path=%.1f chars, compact-strings=%s%n",
          fileCount,
          indexBytes,
          bytesToKiB(indexBytes),
          snapshotBytes,
          bytesToKiB(snapshotBytes),
          totalBytes,
          bytesToKiB(totalBytes),
          (double) totalBytes / fileCount,
          entryBytes,
          keyBytes,
          fileBytes,
          pathBytes,
          (double) pathCharacters / fileCount,
          COMPACT_STRINGS_ENABLED);
    } finally {
      cacheDisabled.close();
      cacheEnabled.close();
    }
  }

  private static void runScenario(int fileCount, int warmupReads, int reads, int rounds)
      throws Exception {
    Assert.assertTrue(fileCount > 0);
    final WALNode cacheDisabled = createWalNode(fileCount, false);
    final WALNode cacheEnabled = createWalNode(fileCount, true);
    try {
      assertFileList(cacheDisabled.getSortedWalFilesForTest(), fileCount);
      assertFileList(cacheEnabled.getSortedWalFilesForTest(), fileCount);

      runReads(cacheDisabled, warmupReads);
      runReads(cacheEnabled, warmupReads);

      final long[] cacheDisabledNanos = new long[rounds];
      final long[] cacheEnabledNanos = new long[rounds];
      for (int round = 0; round < rounds; round++) {
        if ((round & 1) == 0) {
          cacheDisabledNanos[round] = measureReads(cacheDisabled, reads);
          cacheEnabledNanos[round] = measureReads(cacheEnabled, reads);
        } else {
          cacheEnabledNanos[round] = measureReads(cacheEnabled, reads);
          cacheDisabledNanos[round] = measureReads(cacheDisabled, reads);
        }
      }

      final double cacheDisabledNanosPerRead = median(cacheDisabledNanos) / reads;
      final double cacheEnabledNanosPerRead = median(cacheEnabledNanos) / reads;
      System.out.printf(
          Locale.ROOT,
          "WAL sorted-file-list benchmark: files=%d, warmup-reads=%d, reads/round=%d, rounds=%d%n",
          fileCount,
          warmupReads,
          reads,
          rounds);
      printResult("cache=false", cacheDisabledNanosPerRead);
      printResult("cache=true", cacheEnabledNanosPerRead);
      System.out.printf(
          Locale.ROOT,
          "  speedup=%.2fx, latency reduction=%.2f%%%n",
          cacheDisabledNanosPerRead / cacheEnabledNanosPerRead,
          (cacheDisabledNanosPerRead - cacheEnabledNanosPerRead)
              * 100.0
              / cacheDisabledNanosPerRead);
    } finally {
      cacheDisabled.close();
      cacheEnabled.close();
    }
  }

  private static WALNode createWalNode(int fileCount, boolean cacheEnabled) throws IOException {
    final String state = cacheEnabled ? "enabled" : "disabled";
    final String directory = BASE_DIRECTORY + File.separator + fileCount + File.separator + state;
    EnvironmentUtils.cleanDir(directory);
    final File directoryFile = new File(directory);
    Assert.assertTrue(directoryFile.mkdirs() || directoryFile.isDirectory());
    for (int version = 0; version < fileCount - 1; version++) {
      final File walFile =
          new File(
              directoryFile,
              WALFileUtils.getLogFileName(version, version, WALFileStatus.CONTAINS_SEARCH_INDEX));
      Assert.assertTrue(walFile.createNewFile());
    }

    CONFIG.setWalFileListCacheEnabled(cacheEnabled);
    return new WALNode(
        "wal-file-list-cache-performance-" + fileCount + '-' + state,
        directory,
        fileCount - 1L,
        fileCount - 1L);
  }

  private static long measureReads(WALNode walNode, int reads) {
    final long startNanos = System.nanoTime();
    runReads(walNode, reads);
    return System.nanoTime() - startNanos;
  }

  private static void runReads(WALNode walNode, int reads) {
    long checksum = 0;
    for (int i = 0; i < reads; i++) {
      final File[] walFiles = walNode.getSortedWalFilesForTest();
      checksum += walFiles.length;
      checksum += walFiles[walFiles.length - 1].getName().length();
    }
    benchmarkBlackhole = checksum;
  }

  private static void assertFileList(File[] walFiles, int expectedFileCount) {
    Assert.assertEquals(expectedFileCount, walFiles.length);
    for (int i = 0; i < walFiles.length; i++) {
      Assert.assertEquals(i, WALFileUtils.parseVersionId(walFiles[i].getName()));
    }
  }

  private static long getTreeMapEntryShallowSize() {
    final TreeMap<Long, File> sample = new TreeMap<>();
    sample.put(0L, new File("sample.wal"));
    return RamUsageEstimator.shallowSizeOf(sample.entrySet().iterator().next());
  }

  private static long estimateStringBytes(String value) {
    final boolean latin1 = value.chars().allMatch(character -> character <= 0xFF);
    final int bytesPerCharacter = COMPACT_STRINGS_ENABLED && latin1 ? 1 : 2;
    return STRING_SHALLOW_SIZE
        + RamUsageEstimator.sizeOfByteArray(value.length() * bytesPerCharacter);
  }

  private static boolean isCompactStringsEnabled() {
    try {
      return Boolean.parseBoolean(
          ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
              .getVMOption("CompactStrings")
              .getValue());
    } catch (RuntimeException ignored) {
      // The conservative fallback models two bytes per UTF-16 code unit.
      return false;
    }
  }

  private static double bytesToKiB(long bytes) {
    return bytes / 1024.0;
  }

  private static void assumePerformanceTestEnabled() {
    Assume.assumeTrue(
        String.format(
            Locale.ROOT,
            "Manual performance UT. Enable with -D%s=true; optionally tune -D%s, -D%s, -D%s, and -D%s.",
            ENABLED_PROPERTY,
            FILE_COUNTS_PROPERTY,
            WARMUP_READS_PROPERTY,
            READS_PROPERTY,
            ROUNDS_PROPERTY),
        Boolean.getBoolean(ENABLED_PROPERTY));
  }

  private static int[] parseFileCounts() {
    final String configured = System.getProperty(FILE_COUNTS_PROPERTY);
    if (configured == null || configured.trim().isEmpty()) {
      return DEFAULT_FILE_COUNTS;
    }
    return Arrays.stream(configured.split(","))
        .map(String::trim)
        .mapToInt(Integer::parseInt)
        .toArray();
  }

  private static double median(long[] values) {
    Arrays.sort(values);
    final int middle = values.length / 2;
    return (values.length & 1) == 1
        ? values[middle]
        : values[middle - 1] + (values[middle] - values[middle - 1]) / 2.0;
  }

  private static void printResult(String label, double nanosPerRead) {
    System.out.printf(
        Locale.ROOT,
        "  %-12s latency=%.3f us/read, throughput=%.0f reads/s%n",
        label,
        nanosPerRead / 1000.0,
        1_000_000_000.0 / nanosPerRead);
  }
}
