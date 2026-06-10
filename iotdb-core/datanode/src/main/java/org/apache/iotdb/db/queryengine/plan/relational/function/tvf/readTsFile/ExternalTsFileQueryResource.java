/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.readTsFile;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.ExternalTsFileDeviceFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Query-scoped resource for external TsFile scans.
 *
 * <p>Reader instances are owned by {@link FileReaderManager}; this class only balances reference
 * increments/decrements for the query's external TsFile paths and deletes the query temporary
 * directory.
 */
public class ExternalTsFileQueryResource implements AutoCloseable {

  public static final String EXTERNAL_TSFILE_TMP_DIR = "external-tsfile";
  private static final long DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES = 8L * 1024 * 1024;
  private static final long DEVICE_OFFSET_INSTANCE_SIZE_IN_BYTES = 32L;

  private final QueryId queryId;
  private final Path queryTempRoot;
  private final String tableName;
  private final List<String> tsFilePaths;
  private final LongConsumer ioSizeRecorder;
  private final List<DeviceEntry> deviceEntries = new ArrayList<>();
  private List<DeviceTaskPartition> deviceTaskPartitions = Collections.emptyList();
  private Comparator<DeviceEntry> deviceEntryComparator;

  private boolean readersRetained;
  private boolean closed;

  public ExternalTsFileQueryResource(
      QueryId queryId,
      Path tempRoot,
      String tableName,
      List<String> tsFilePaths,
      LongConsumer ioSizeRecorder,
      boolean useExactTempRoot) {
    this.queryId = queryId;
    this.queryTempRoot =
        useExactTempRoot
            ? requireNonNull(tempRoot, "tempRoot is null")
            : requireNonNull(tempRoot, "tempRoot is null")
                .resolve(requireNonNull(queryId, "queryId is null").getId());
    this.tableName = tableName;
    this.tsFilePaths =
        Collections.unmodifiableList(new ArrayList<>(requireNonNull(tsFilePaths, "tsFilePaths")));
    this.ioSizeRecorder = requireNonNull(ioSizeRecorder, "ioSizeRecorder is null");
  }

  public synchronized void collectDeviceEntries(
      SchemaFilter schemaFilter, Comparator<DeviceEntry> comparator, int partitionCount) {
    checkNotClosed();
    retainFileReaderReferences();
    ExternalTsFileDeviceFilterVisitor deviceFilterVisitor = new ExternalTsFileDeviceFilterVisitor();
    try (DeviceCollector deviceCollector = new DeviceCollector()) {
      List<DeviceTaskPartition> partitions = createDeviceTaskPartitions(partitionCount);
      while (deviceCollector.hasNextDevice()) {
        IDeviceID deviceID = deviceCollector.nextDevice();
        if (schemaFilter != null
            && !Boolean.TRUE.equals(schemaFilter.accept(deviceFilterVisitor, deviceID))) {
          continue;
        }
        DeviceEntry deviceEntry = new AlignedDeviceEntry(deviceID, new Binary[0]);
        int deviceEntryIndex = deviceEntries.size();
        deviceEntries.add(deviceEntry);
        DeviceTask deviceTask =
            new DeviceTask(
                deviceEntryIndex, new ArrayList<>(deviceCollector.getCurrentDeviceOffsets()));
        DeviceTaskPartition partition =
            partitions.get(Math.floorMod(deviceID.hashCode(), partitions.size()));
        partition.add(deviceTask);
        if (partition.getEstimatedSizeInBytes() >= DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES) {
          partition.flush(comparator);
        }
      }
      deviceEntryComparator = comparator;
      collectDeviceTaskPartitions(partitions, comparator);
    }
  }

  public synchronized MultiWayMergeReader getMultiWayMergeReader(int partitionIndex) {
    checkNotClosed();
    DeviceTaskPartition partition = getDeviceTaskPartition(partitionIndex);
    try {
      return new DeviceTaskRunReader(partition.getRunFiles(), deviceEntries, deviceEntryComparator);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create external TsFile device task run reader", e);
    }
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public List<DeviceEntry> getDeviceEntries() {
    return deviceEntries;
  }

  public List<DeviceTaskPartition> getDeviceTaskPartitions() {
    return deviceTaskPartitions;
  }

  private DeviceTaskPartition getDeviceTaskPartition(int partitionIndex) {
    for (DeviceTaskPartition partition : deviceTaskPartitions) {
      if (partition.getPartitionIndex() == partitionIndex) {
        return partition;
      }
    }
    throw new IllegalArgumentException(
        "Unknown external TsFile device task partition: " + partitionIndex);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    releaseFileReaderReferences();

    if (Files.exists(queryTempRoot)) {
      FileUtils.deleteFileOrDirectory(queryTempRoot.toFile(), true);
    }
  }

  private void retainFileReaderReferences() {
    if (readersRetained) {
      return;
    }
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().increaseExternalFileReaderReference(tsFilePath);
    }
    readersRetained = true;
  }

  private void releaseFileReaderReferences() {
    if (!readersRetained) {
      return;
    }
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().decreaseExternalFileReaderReference(tsFilePath);
    }
    readersRetained = false;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("External TsFile query resource has been closed: " + queryId);
    }
  }

  public interface MultiWayMergeReader extends AutoCloseable {
    boolean hasNextDevice() throws IOException;

    DeviceEntry nextDevice() throws IOException;

    List<DeviceOffset> getCurrentDeviceOffsets();

    @Override
    void close() throws IOException;
  }

  public class DeviceTaskPartition {

    private final int partitionIndex;
    private final List<DeviceTask> pendingDeviceTasks = new ArrayList<>();
    private final List<Integer> deviceEntryIndexes = new ArrayList<>();
    private final List<Path> runFiles = new ArrayList<>();
    private long estimatedSizeInBytes;

    private DeviceTaskPartition(int partitionIndex) {
      this.partitionIndex = partitionIndex;
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public List<Integer> getDeviceEntryIndexes() {
      return deviceEntryIndexes;
    }

    private void add(DeviceTask deviceTask) {
      pendingDeviceTasks.add(deviceTask);
      estimatedSizeInBytes += estimateDeviceTaskSize(deviceTask);
    }

    private void flush(Comparator<DeviceEntry> comparator) {
      if (pendingDeviceTasks.isEmpty()) {
        return;
      }
      sortPendingDeviceTasks(comparator);
      try {
        runFiles.add(
            writeDeviceTaskRun(
                queryTempRoot.resolve("child-" + partitionIndex),
                runFiles.size(),
                pendingDeviceTasks));
      } catch (IOException e) {
        throw new RuntimeException("Failed to flush external TsFile device task partition", e);
      }
      for (DeviceTask deviceTask : pendingDeviceTasks) {
        deviceEntryIndexes.add(deviceTask.deviceEntryIndex);
      }
      pendingDeviceTasks.clear();
      estimatedSizeInBytes = 0;
    }

    private void sortPendingDeviceTasks(Comparator<DeviceEntry> comparator) {
      if (comparator != null) {
        pendingDeviceTasks.sort(
            (left, right) ->
                comparator.compare(
                    deviceEntries.get(left.deviceEntryIndex),
                    deviceEntries.get(right.deviceEntryIndex)));
      } else {
        pendingDeviceTasks.sort(
            (left, right) ->
                deviceEntries
                    .get(left.deviceEntryIndex)
                    .getDeviceID()
                    .compareTo(deviceEntries.get(right.deviceEntryIndex).getDeviceID()));
      }
    }

    private long getEstimatedSizeInBytes() {
      return estimatedSizeInBytes;
    }

    private boolean hasDeviceTasks() {
      return !deviceEntryIndexes.isEmpty();
    }

    private void sortDeviceEntries(Comparator<DeviceEntry> comparator) {
      if (comparator != null) {
        deviceEntryIndexes.sort(
            (left, right) -> comparator.compare(deviceEntries.get(left), deviceEntries.get(right)));
      } else {
        deviceEntryIndexes.sort(
            (left, right) ->
                deviceEntries
                    .get(left)
                    .getDeviceID()
                    .compareTo(deviceEntries.get(right).getDeviceID()));
      }
    }

    private List<Path> getRunFiles() {
      return runFiles;
    }
  }

  private List<DeviceTaskPartition> createDeviceTaskPartitions(int partitionCount) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException(
          "External TsFile device task partition count must be positive");
    }
    List<DeviceTaskPartition> partitions = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(new DeviceTaskPartition(i));
    }
    return partitions;
  }

  private void collectDeviceTaskPartitions(
      List<DeviceTaskPartition> partitions, Comparator<DeviceEntry> comparator) {
    if (partitions.isEmpty()) {
      deviceTaskPartitions = Collections.emptyList();
      return;
    }
    List<DeviceTaskPartition> nonEmptyPartitions = new ArrayList<>(partitions.size());
    for (DeviceTaskPartition partition : partitions) {
      partition.flush(comparator);
      if (!partition.hasDeviceTasks()) {
        continue;
      }
      partition.sortDeviceEntries(comparator);
      nonEmptyPartitions.add(partition);
    }
    deviceTaskPartitions = nonEmptyPartitions;
  }

  private Path writeDeviceTaskRun(Path runRoot, int runIndex, List<DeviceTask> deviceTasks)
      throws IOException {
    Files.createDirectories(runRoot);
    Path runFile = runRoot.resolve("run-" + runIndex + ".bin");
    try (DataOutputStream outputStream =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(runFile)))) {
      ReadWriteIOUtils.write(deviceTasks.size(), outputStream);
      for (DeviceTask deviceTask : deviceTasks) {
        deviceTask.serialize(outputStream);
      }
    }
    return runFile;
  }

  private static long estimateDeviceTaskSize(DeviceTask deviceTask) {
    long size = 64L;
    for (DeviceOffset offset : deviceTask.deviceOffsets) {
      size +=
          DEVICE_OFFSET_INSTANCE_SIZE_IN_BYTES
              + (long) Long.BYTES * offset.measurementNodeOffset.length;
    }
    return size;
  }

  private static class DeviceTaskRunReader implements MultiWayMergeReader {

    private final List<DeviceEntry> deviceEntries;
    private final PriorityQueue<DeviceTaskRunCursor> runCursors;
    private DeviceTask nextDeviceTask;
    private List<DeviceOffset> currentDeviceOffsets = Collections.emptyList();

    private DeviceTaskRunReader(
        List<Path> runFiles, List<DeviceEntry> deviceEntries, Comparator<DeviceEntry> comparator)
        throws IOException {
      this.deviceEntries = deviceEntries;
      Comparator<DeviceTaskRunCursor> cursorComparator =
          (left, right) ->
              comparator == null
                  ? left.getCurrentDeviceEntry()
                      .getDeviceID()
                      .compareTo(right.getCurrentDeviceEntry().getDeviceID())
                  : comparator.compare(left.getCurrentDeviceEntry(), right.getCurrentDeviceEntry());
      this.runCursors = new PriorityQueue<>(cursorComparator);
      for (Path runFile : runFiles) {
        DeviceTaskRunCursor cursor = new DeviceTaskRunCursor(runFile, deviceEntries);
        if (cursor.hasCurrentDeviceTask()) {
          runCursors.add(cursor);
        } else {
          cursor.close();
        }
      }
    }

    @Override
    public boolean hasNextDevice() throws IOException {
      if (nextDeviceTask != null) {
        return true;
      }
      nextDeviceTask = readNextDeviceTask();
      return nextDeviceTask != null;
    }

    @Override
    public DeviceEntry nextDevice() throws IOException {
      if (!hasNextDevice()) {
        throw new EOFException("No more external TsFile device task");
      }
      DeviceTask deviceTask = nextDeviceTask;
      nextDeviceTask = null;
      currentDeviceOffsets = deviceTask.deviceOffsets;
      return deviceEntries.get(deviceTask.deviceEntryIndex);
    }

    @Override
    public List<DeviceOffset> getCurrentDeviceOffsets() {
      return currentDeviceOffsets;
    }

    @Override
    public void close() throws IOException {
      IOException exception = null;
      while (!runCursors.isEmpty()) {
        try {
          runCursors.poll().close();
        } catch (IOException e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
      if (exception != null) {
        throw exception;
      }
    }

    private DeviceTask readNextDeviceTask() throws IOException {
      if (runCursors.isEmpty()) {
        return null;
      }
      DeviceTaskRunCursor cursor = runCursors.poll();
      DeviceTask result = cursor.getCurrentDeviceTask();
      cursor.advance();
      if (cursor.hasCurrentDeviceTask()) {
        runCursors.add(cursor);
      } else {
        cursor.close();
      }
      return result;
    }
  }

  private static class DeviceTaskRunCursor implements Closeable {

    private final List<DeviceEntry> deviceEntries;
    private final DataInputStream inputStream;
    private int remainingDeviceTasks;
    private DeviceTask currentDeviceTask;

    private DeviceTaskRunCursor(Path runFile, List<DeviceEntry> deviceEntries) throws IOException {
      this.deviceEntries = deviceEntries;
      this.inputStream =
          new DataInputStream(new BufferedInputStream(Files.newInputStream(runFile)));
      this.remainingDeviceTasks = ReadWriteIOUtils.readInt(inputStream);
      advance();
    }

    private void advance() throws IOException {
      if (remainingDeviceTasks <= 0) {
        currentDeviceTask = null;
        return;
      }
      remainingDeviceTasks--;
      currentDeviceTask = DeviceTask.deserialize(inputStream);
    }

    private boolean hasCurrentDeviceTask() {
      return currentDeviceTask != null;
    }

    private DeviceTask getCurrentDeviceTask() {
      return currentDeviceTask;
    }

    private DeviceEntry getCurrentDeviceEntry() {
      return deviceEntries.get(currentDeviceTask.deviceEntryIndex);
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }

  private class DeviceCollector implements Closeable {

    private final Map<Integer, LazyTsFileDeviceIterator> deviceIteratorMap = new HashMap<>();

    private IDeviceID currentDevice;
    private List<DeviceOffset> currentDeviceOffsets = Collections.emptyList();

    private DeviceCollector() {
      try {
        for (int fileIndex = 0; fileIndex < tsFilePaths.size(); fileIndex++) {
          TsFileSequenceReader reader =
              FileReaderManager.getInstance()
                  .get(tsFilePaths.get(fileIndex), null, true, ioSizeRecorder, true);
          deviceIteratorMap.put(
              fileIndex, new LazyTsFileDeviceIterator(reader, tableName, ioSizeRecorder));
        }
      } catch (IOException e) {
        close();
        throw new RuntimeException("Failed to create external TsFile device collector", e);
      }
    }

    private boolean hasNextDevice() {
      for (LazyTsFileDeviceIterator deviceIterator : deviceIteratorMap.values()) {
        if (deviceIterator.hasNext()
            || (deviceIterator.hasCurrent()
                && !deviceIterator.getCurrentDeviceID().equals(currentDevice))) {
          return true;
        }
      }
      return false;
    }

    private IDeviceID nextDevice() {
      IDeviceID minDevice = null;
      Iterator<Map.Entry<Integer, LazyTsFileDeviceIterator>> iterator =
          deviceIteratorMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, LazyTsFileDeviceIterator> entry = iterator.next();
        LazyTsFileDeviceIterator deviceIterator = entry.getValue();
        IDeviceID currentFileDevice = null;
        if (deviceIterator.hasCurrent()) {
          currentFileDevice = deviceIterator.getCurrentDeviceID();
        }
        if (currentFileDevice == null || currentFileDevice.equals(currentDevice)) {
          if (deviceIterator.hasNext()) {
            currentFileDevice = deviceIterator.next();
          } else {
            iterator.remove();
            continue;
          }
        }
        if (minDevice == null || minDevice.compareTo(currentFileDevice) > 0) {
          minDevice = currentFileDevice;
        }
      }
      currentDevice = minDevice;
      collectCurrentDeviceOffsets();
      return currentDevice;
    }

    private void collectCurrentDeviceOffsets() {
      List<DeviceOffset> deviceOffsets = new ArrayList<>();
      for (Map.Entry<Integer, LazyTsFileDeviceIterator> entry : deviceIteratorMap.entrySet()) {
        LazyTsFileDeviceIterator deviceIterator = entry.getValue();
        if (currentDevice != null
            && deviceIterator.hasCurrent()
            && currentDevice.equals(deviceIterator.getCurrentDeviceID())) {
          deviceOffsets.add(
              new DeviceOffset(
                  entry.getKey(), deviceIterator.getCurrentDeviceMeasurementNodeOffset()));
        }
      }
      currentDeviceOffsets = deviceOffsets;
    }

    private List<DeviceOffset> getCurrentDeviceOffsets() {
      return currentDeviceOffsets;
    }

    @Override
    public void close() {
      deviceIteratorMap.clear();
      currentDeviceOffsets = Collections.emptyList();
    }
  }

  private static class DeviceTask {

    private final int deviceEntryIndex;
    private final List<DeviceOffset> deviceOffsets;

    private DeviceTask(int deviceEntryIndex, List<DeviceOffset> deviceOffsets) {
      this.deviceEntryIndex = deviceEntryIndex;
      this.deviceOffsets = deviceOffsets;
    }

    private void serialize(DataOutputStream outputStream) throws IOException {
      ReadWriteIOUtils.write(deviceEntryIndex, outputStream);
      ReadWriteIOUtils.write(deviceOffsets.size(), outputStream);
      for (DeviceOffset offset : deviceOffsets) {
        ReadWriteIOUtils.write(offset.fileIndex, outputStream);
        ReadWriteIOUtils.write(offset.measurementNodeOffset.length, outputStream);
        for (long measurementNodeOffset : offset.measurementNodeOffset) {
          ReadWriteIOUtils.write(measurementNodeOffset, outputStream);
        }
      }
    }

    private static DeviceTask deserialize(DataInputStream inputStream) throws IOException {
      int deviceEntryIndex = ReadWriteIOUtils.readInt(inputStream);
      int offsetSize = ReadWriteIOUtils.readInt(inputStream);
      List<DeviceOffset> offsets = new ArrayList<>(offsetSize);
      for (int i = 0; i < offsetSize; i++) {
        int fileIndex = ReadWriteIOUtils.readInt(inputStream);
        int measurementNodeOffsetLength = ReadWriteIOUtils.readInt(inputStream);
        long[] measurementNodeOffset = new long[measurementNodeOffsetLength];
        for (int j = 0; j < measurementNodeOffsetLength; j++) {
          measurementNodeOffset[j] = inputStream.readLong();
        }
        offsets.add(new DeviceOffset(fileIndex, measurementNodeOffset));
      }
      return new DeviceTask(deviceEntryIndex, offsets);
    }
  }

  public static class DeviceOffset {

    private final int fileIndex;
    private final long[] measurementNodeOffset;

    private DeviceOffset(int fileIndex, long[] measurementNodeOffset) {
      this.fileIndex = fileIndex;
      this.measurementNodeOffset = measurementNodeOffset;
    }

    public int getFileIndex() {
      return fileIndex;
    }

    public long[] getMeasurementNodeOffset() {
      return measurementNodeOffset;
    }
  }
}
