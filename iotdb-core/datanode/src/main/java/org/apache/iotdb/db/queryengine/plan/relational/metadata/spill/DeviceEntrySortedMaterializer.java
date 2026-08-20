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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.external.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Materializes a sorted data set in memory or through sorted runs and a K-way merge. */
public final class DeviceEntrySortedMaterializer extends AbstractDeviceEntryMaterializer {

  private static final int MAX_MERGE_FAN_IN = 32;

  private final Comparator<DeviceEntry> comparator;
  private final boolean distinct;
  private final List<List<Path>> sortedRuns = new ArrayList<>();

  private Path runDirectory;

  public DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator) {
    this(queryId, planNodeId, bufferSizeInBytes, comparator, false);
  }

  public DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator,
      boolean distinct) {
    this(queryId, planNodeId, bufferSizeInBytes, comparator, distinct, false);
  }

  private DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator,
      boolean distinct,
      boolean rawSegment) {
    super(queryId, planNodeId, bufferSizeInBytes, rawSegment);
    this.comparator = comparator;
    this.distinct = distinct;
  }

  public DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator,
      MPPQueryContext queryContext) {
    this(queryId, planNodeId, bufferSizeInBytes, comparator);
    setQueryContext(queryContext);
  }

  public DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator,
      boolean distinct,
      MPPQueryContext queryContext) {
    this(queryId, planNodeId, bufferSizeInBytes, comparator, distinct);
    setQueryContext(queryContext);
  }

  public DeviceEntrySortedMaterializer(
      String queryId,
      PlanNodeId planNodeId,
      long bufferSizeInBytes,
      Comparator<DeviceEntry> comparator,
      boolean distinct,
      boolean rawSegment,
      MPPQueryContext queryContext) {
    this(queryId, planNodeId, bufferSizeInBytes, comparator, distinct, rawSegment);
    setQueryContext(queryContext);
  }

  @Override
  public void append(DeviceEntry entry) throws IOException {
    checkNotFinished();
    appendToBuffer(entry);
  }

  @Override
  public long appendWithMemoryControl(DeviceEntry entry) throws IOException {
    checkNotFinished();
    long entryRamBytes = entry.ramBytesUsed();
    if (!isBufferEmpty() && getBufferedRamBytes() + entryRamBytes > thresholdInBytes()) {
      long releasedRamBytes = getBufferedRamBytes();
      flushRun();
      appendToBuffer(entry);
      addBufferedRamBytes(entryRamBytes);
      return releasedRamBytes;
    }
    appendToBuffer(entry);
    addBufferedRamBytes(entryRamBytes);
    return 0;
  }

  @Override
  public boolean isSpilled() {
    return !sortedRuns.isEmpty();
  }

  @Override
  public void forceSpill() throws IOException {
    checkNotFinished();
    flushRun();
  }

  @Override
  public DeviceEntryDataSet finish() throws IOException {
    checkNotFinished();
    checkTimeout();
    if (entryCount() == 0) {
      DeviceEntryDataSet dataSet = new InMemoryDeviceEntryDataSet(copyBufferedEntries());
      recordDeviceEntryCount();
      markFinished();
      return dataSet;
    }

    DeviceEntryDataSet dataSet;
    if (sortedRuns.isEmpty()) {
      if (distinct) {
        // Only need to deduplicate instead of sorting here
        List<DeviceEntry> distinctEntries = distinctBufferedEntries();
        setEntryCount(distinctEntries.size());
        dataSet = new InMemoryDeviceEntryDataSet(distinctEntries);
      } else {
        sortBufferedEntries(comparator);
        checkTimeout();
        dataSet = new InMemoryDeviceEntryDataSet(copyBufferedEntries());
      }
      recordDeviceEntryCount();
      markFinished();
      return dataSet;
    }

    try {
      flushRun();
      List<List<Path>> finalRuns = compactRuns(new ArrayList<>(sortedRuns));
      Path finalDirectory = spillDirectory(ownerDirectory());
      List<Path> finalSegments;
      int finalEntryCount;
      try (DeviceEntryDiskSpiller outputSpiller = createSpiller(finalDirectory)) {
        if (finalRuns.size() == 1) {
          finalEntryCount = copyRun(finalRuns.get(0), outputSpiller);
        } else {
          finalEntryCount = mergeRuns(finalRuns, outputSpiller);
        }
        finalSegments = outputSpiller.finish();
      }
      setEntryCount(finalEntryCount);
      dataSet =
          new SpilledDeviceEntryDataSet(queryId(), ownerDirectory(), finalSegments, entryCount());
      recordDeviceEntryCount();
      markFinished();
      deleteRunDirectoryBestEffort();
      return dataSet;
    } catch (IOException | RuntimeException e) {
      try {
        cleanupOwnerDirectory();
      } catch (IOException cleanupException) {
        e.addSuppressed(cleanupException);
      }
      throw e;
    }
  }

  private void flushRun() throws IOException {
    if (isBufferEmpty()) {
      return;
    }
    ensureSpillDirectory();
    sortBufferedEntries(comparator);
    checkTimeout();
    Path currentRunDirectory = runDirectory.resolve(String.format("run-%06d", sortedRuns.size()));
    try (DeviceEntryDiskSpiller runSpiller = createSpiller(currentRunDirectory)) {
      for (DeviceEntry entry : bufferedEntries()) {
        runSpiller.append(entry.serializeToBytes());
      }
      sortedRuns.add(runSpiller.finish());
    }
    clearBuffer();
  }

  private void ensureSpillDirectory() throws IOException {
    if (ownerDirectory() != null) {
      return;
    }
    runDirectory = ensureOwnerDirectory().resolve("sort-run");
  }

  private int copyRun(List<Path> run, DeviceEntryDiskSpiller outputSpiller) throws IOException {
    int outputCount = 0;
    DeviceEntry previous = null;
    try (DeviceEntryFileSpillerReader reader = createReader(run, true)) {
      while (reader.hasNext()) {
        DeviceEntry entry = reader.next();
        if (!distinct || previous == null || comparator.compare(previous, entry) != 0) {
          outputSpiller.append(entry.serializeToBytes());
          previous = entry;
          outputCount++;
        }
      }
    }
    return outputCount;
  }

  private void deleteRunDirectoryBestEffort() {
    try {
      FileUtils.deleteDirectory(runDirectory.toFile());
    } catch (IOException ignored) {
      // Query cleanup removes the published data set and any remaining runs.
    }
  }

  private List<List<Path>> compactRuns(List<List<Path>> runs) throws IOException {
    int level = 1;
    while (runs.size() > MAX_MERGE_FAN_IN) {
      List<List<Path>> nextRuns = new ArrayList<>();
      for (int from = 0, group = 0; from < runs.size(); from += MAX_MERGE_FAN_IN, group++) {
        int to = Math.min(from + MAX_MERGE_FAN_IN, runs.size());
        List<List<Path>> runGroup = new ArrayList<>(runs.subList(from, to));
        if (runGroup.size() == 1) {
          nextRuns.add(runGroup.get(0));
          continue;
        }
        Path outputDirectory =
            runDirectory
                .resolve(String.format("level-%06d", level))
                .resolve(String.format("run-%06d", group));
        try (DeviceEntryDiskSpiller outputSpiller = createSpiller(outputDirectory)) {
          mergeRuns(runGroup, outputSpiller);
          nextRuns.add(outputSpiller.finish());
        }
      }
      runs = nextRuns;
      level++;
    }
    return runs;
  }

  private int mergeRuns(List<List<Path>> runs, DeviceEntryDiskSpiller outputSpiller)
      throws IOException {
    List<DeviceEntryFileSpillerReader> readers = new ArrayList<>(runs.size());
    PriorityQueue<MergeElement> queue =
        new PriorityQueue<>(
            (left, right) -> {
              int result = comparator.compare(left.entry, right.entry);
              return result != 0 ? result : Integer.compare(left.readerIndex, right.readerIndex);
            });
    Throwable failure = null;
    int outputCount = 0;
    DeviceEntry previous = null;
    try {
      for (int i = 0; i < runs.size(); i++) {
        DeviceEntryFileSpillerReader reader = createReader(runs.get(i), true);
        readers.add(reader);
        if (reader.hasNext()) {
          queue.add(new MergeElement(reader.next(), i));
        }
      }
      while (!queue.isEmpty()) {
        MergeElement element = queue.poll();
        if (!distinct || previous == null || comparator.compare(previous, element.entry) != 0) {
          outputSpiller.append(element.entry.serializeToBytes());
          previous = element.entry;
          outputCount++;
        }
        DeviceEntryFileSpillerReader reader = readers.get(element.readerIndex);
        if (reader.hasNext()) {
          queue.add(new MergeElement(reader.next(), element.readerIndex));
        }
      }
    } catch (IOException | RuntimeException | Error e) {
      failure = e;
      throw e;
    } finally {
      IOException closeException = null;
      for (DeviceEntryFileSpillerReader reader : readers) {
        try {
          reader.close();
        } catch (IOException e) {
          if (closeException == null) {
            closeException = e;
          } else {
            closeException.addSuppressed(e);
          }
        }
      }
      if (closeException != null) {
        if (failure != null) {
          failure.addSuppressed(closeException);
        } else {
          throw closeException;
        }
      }
    }
    return outputCount;
  }

  private static final class MergeElement {
    private final DeviceEntry entry;
    private final int readerIndex;

    private MergeElement(DeviceEntry entry, int readerIndex) {
      this.entry = entry;
      this.readerIndex = readerIndex;
    }
  }
}
