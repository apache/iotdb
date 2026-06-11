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

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.ExternalTsFileDeviceFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
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

  private final QueryId queryId;
  private final MPPQueryContext queryContext;
  private final Path queryTempRoot;
  private final String tableName;
  private final List<String> tsFilePaths;
  private final List<TsFileResource> tsFileResources;
  private final LongConsumer ioSizeRecorder;
  private final List<DeviceEntry> deviceEntries = new ArrayList<>();
  private final List<DeviceTaskPartition> deviceTaskPartitions = new ArrayList<>();
  private Comparator<DeviceEntry> deviceEntryComparator;

  private volatile boolean closed;

  public ExternalTsFileQueryResource(
      MPPQueryContext queryContext,
      Path tempRoot,
      String tableName,
      List<String> tsFilePaths,
      LongConsumer ioSizeRecorder,
      boolean useExactTempRoot) {
    this.queryContext = requireNonNull(queryContext, "queryContext is null");
    this.queryId = queryContext.getQueryId();
    this.queryTempRoot =
        useExactTempRoot
            ? requireNonNull(tempRoot, "tempRoot is null")
            : requireNonNull(tempRoot, "tempRoot is null").resolve(this.queryId.getId());
    this.tableName = tableName;
    this.tsFilePaths =
        Collections.unmodifiableList(new ArrayList<>(requireNonNull(tsFilePaths, "tsFilePaths")));
    this.tsFileResources = createTsFileResources(this.tsFilePaths);
    this.ioSizeRecorder = requireNonNull(ioSizeRecorder, "ioSizeRecorder is null");
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().increaseExternalFileReaderReference(tsFilePath);
    }
  }

  public void collectDeviceEntries(
      SchemaFilter schemaFilter, Comparator<DeviceEntry> comparator, int partitionCount) {
    checkNotClosed();
    ExternalTsFileDeviceFilterVisitor deviceFilterVisitor = new ExternalTsFileDeviceFilterVisitor();
    try (DeviceCollector deviceCollector = new DeviceCollector()) {
      createDeviceTaskPartitions(partitionCount);
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
            new DeviceTask(deviceEntryIndex, deviceCollector.getCurrentDeviceOffsets());
        DeviceTaskPartition partition =
            deviceTaskPartitions.get(
                Math.floorMod(deviceID.hashCode(), deviceTaskPartitions.size()));
        partition.add(deviceTask);
        if (partition.shouldFlush()) {
          partition.flush(comparator);
        }
      }
      deviceEntryComparator = comparator;
      collectDeviceTaskPartitions(comparator);
    }
  }

  public DeviceTaskRunReader getDeviceTaskRunReader(int partitionIndex) {
    checkNotClosed();
    DeviceTaskPartition partition = getDeviceTaskPartition(partitionIndex);
    try {
      return new DeviceTaskRunReader(partition);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create external TsFile device task run reader", e);
    }
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public List<TsFileResource> getTsFileResources() {
    return tsFileResources;
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
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    releaseFileReaderReferences();

    if (Files.exists(queryTempRoot)) {
      FileUtils.deleteFileOrDirectory(queryTempRoot.toFile(), true);
    }
  }

  private void releaseFileReaderReferences() {
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().decreaseExternalFileReaderReference(tsFilePath);
    }
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("External TsFile query resource has been closed: " + queryId);
    }
  }

  private static List<TsFileResource> createTsFileResources(List<String> tsFilePaths) {
    List<TsFileResource> tsFileResources = new ArrayList<>(tsFilePaths.size());
    for (String tsFilePath : tsFilePaths) {
      TsFileResource resource =
          new TsFileResource(new File(tsFilePath), TsFileResourceStatus.NORMAL);
      if (resource.resourceFileExists()) {
        try {
          resource.deserialize();
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to deserialize external TsFile resource: "
                  + tsFilePath
                  + ", "
                  + e.getMessage(),
              e);
        }
      } else {
        resource.setTimeIndex(new FileTimeIndex(Long.MIN_VALUE, Long.MAX_VALUE));
      }
      tsFileResources.add(resource);
    }
    return Collections.unmodifiableList(tsFileResources);
  }

  public class DeviceTaskPartition {

    private static final long DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES = 8L * 1024 * 1024;
    private static final long MEMORY_RESERVE_BATCH_SIZE_IN_BYTES = 1024 * 1024;

    private final int partitionIndex;
    private final List<DeviceTask> pendingDeviceTasks = new ArrayList<>();
    private final List<Integer> deviceEntryIndexes = new ArrayList<>();
    private final List<Path> runFiles = new ArrayList<>();
    private long reservedBytes;
    private long unreservedBytes;

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
      unreservedBytes += deviceTask.ramBytesUsed();
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
      releaseDeviceTaskMemory();
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

    private boolean shouldFlush() {
      if (getPendingMemoryBytes() >= DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES) {
        return true;
      }
      if (unreservedBytes < MEMORY_RESERVE_BATCH_SIZE_IN_BYTES) {
        return false;
      }
      return !reserveUnreservedMemory();
    }

    private boolean reserveUnreservedMemory() {
      try {
        queryContext.reserveMemoryForFrontEndImmediately(unreservedBytes);
      } catch (MemoryNotEnoughException e) {
        return false;
      }
      reservedBytes += unreservedBytes;
      unreservedBytes = 0;
      return true;
    }

    private long getPendingMemoryBytes() {
      return reservedBytes + unreservedBytes;
    }

    private void releaseDeviceTaskMemory() {
      if (reservedBytes != 0) {
        queryContext.releaseMemoryReservedForFrontEnd(reservedBytes);
        reservedBytes = 0;
      }
      unreservedBytes = 0;
    }

    private boolean hasDeviceTasks() {
      return !deviceEntryIndexes.isEmpty();
    }

    private void finish(Comparator<DeviceEntry> comparator) {
      if (pendingDeviceTasks.isEmpty()) {
        return;
      }
      sortPendingDeviceTasks(comparator);
      for (DeviceTask deviceTask : pendingDeviceTasks) {
        deviceEntryIndexes.add(deviceTask.deviceEntryIndex);
      }
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

    private List<DeviceTask> getPendingDeviceTasks() {
      return pendingDeviceTasks;
    }
  }

  private void createDeviceTaskPartitions(int partitionCount) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException(
          "External TsFile device task partition count must be positive");
    }
    for (int i = 0; i < partitionCount; i++) {
      deviceTaskPartitions.add(new DeviceTaskPartition(i));
    }
  }

  private void collectDeviceTaskPartitions(Comparator<DeviceEntry> comparator) {
    Iterator<DeviceTaskPartition> iterator = deviceTaskPartitions.iterator();
    while (iterator.hasNext()) {
      DeviceTaskPartition partition = iterator.next();
      partition.finish(comparator);
      if (!partition.hasDeviceTasks()) {
        iterator.remove();
        continue;
      }
      partition.sortDeviceEntries(comparator);
    }
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

  public class DeviceTaskRunReader implements AutoCloseable {

    private final PriorityQueue<DeviceTaskRunCursor> runCursors;
    private DeviceEntry currentDevice;
    private ExternalTsFileQueryDataSource currentDeviceQueryDataSource;
    private Map<TsFileResource, DeviceOffset> currentDeviceOffsetMap;

    private DeviceTaskRunReader(DeviceTaskPartition partition) throws IOException {
      Comparator<DeviceTaskRunCursor> cursorComparator =
          (left, right) ->
              deviceEntryComparator == null
                  ? left.getCurrentDeviceEntry()
                      .getDeviceID()
                      .compareTo(right.getCurrentDeviceEntry().getDeviceID())
                  : deviceEntryComparator.compare(
                      left.getCurrentDeviceEntry(), right.getCurrentDeviceEntry());
      this.runCursors = new PriorityQueue<>(cursorComparator);
      for (Path runFile : partition.getRunFiles()) {
        DeviceTaskRunCursor cursor = new DiskDeviceTaskRunCursor(runFile, deviceEntries);
        if (cursor.hasCurrentDeviceTask()) {
          runCursors.add(cursor);
        } else {
          cursor.close();
        }
      }
      DeviceTaskRunCursor memoryCursor =
          new MemoryDeviceTaskRunCursor(partition.getPendingDeviceTasks(), deviceEntries);
      if (memoryCursor.hasCurrentDeviceTask()) {
        runCursors.add(memoryCursor);
      }
    }

    public boolean nextDevice() throws IOException {
      if (runCursors.isEmpty()) {
        return false;
      }
      DeviceTaskRunCursor cursor = runCursors.poll();
      DeviceTask result = cursor.getCurrentDeviceTask();
      cursor.advance();
      if (cursor.hasCurrentDeviceTask()) {
        runCursors.add(cursor);
      } else {
        cursor.close();
      }

      currentDevice = deviceEntries.get(result.deviceEntryIndex);
      List<TsFileResource> unseqResources = new ArrayList<>(result.deviceOffsets.size());
      currentDeviceOffsetMap = new HashMap<>(result.deviceOffsets.size());
      for (DeviceOffset deviceOffset : result.deviceOffsets) {
        TsFileResource tsFileResource = tsFileResources.get(deviceOffset.getFileIndex());
        unseqResources.add(tsFileResource);
        currentDeviceOffsetMap.put(tsFileResource, deviceOffset);
      }
      currentDeviceQueryDataSource =
          new ExternalTsFileQueryDataSource(ExternalTsFileQueryResource.this, unseqResources);
      return true;
    }

    public DeviceEntry getCurrentDevice() {
      return currentDevice;
    }

    public ExternalTsFileQueryDataSource getCurrentDeviceQueryDataSource() {
      return currentDeviceQueryDataSource;
    }

    public Map<TsFileResource, DeviceOffset> getCurrentDeviceOffsetMap() {
      return currentDeviceOffsetMap;
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
  }

  private interface DeviceTaskRunCursor extends Closeable {

    boolean hasCurrentDeviceTask();

    DeviceTask getCurrentDeviceTask();

    DeviceEntry getCurrentDeviceEntry();

    void advance() throws IOException;
  }

  private static class DiskDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final List<DeviceEntry> deviceEntries;
    private final DataInputStream inputStream;
    private int remainingDeviceTasks;
    private DeviceTask currentDeviceTask;

    private DiskDeviceTaskRunCursor(Path runFile, List<DeviceEntry> deviceEntries)
        throws IOException {
      this.deviceEntries = deviceEntries;
      this.inputStream =
          new DataInputStream(new BufferedInputStream(Files.newInputStream(runFile)));
      this.remainingDeviceTasks = ReadWriteIOUtils.readInt(inputStream);
      advance();
    }

    @Override
    public void advance() throws IOException {
      if (remainingDeviceTasks <= 0) {
        currentDeviceTask = null;
        return;
      }
      remainingDeviceTasks--;
      currentDeviceTask = DeviceTask.deserialize(inputStream);
    }

    @Override
    public boolean hasCurrentDeviceTask() {
      return currentDeviceTask != null;
    }

    @Override
    public DeviceTask getCurrentDeviceTask() {
      return currentDeviceTask;
    }

    @Override
    public DeviceEntry getCurrentDeviceEntry() {
      return deviceEntries.get(currentDeviceTask.deviceEntryIndex);
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }

  private static class MemoryDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final List<DeviceTask> deviceTasks;
    private final List<DeviceEntry> deviceEntries;
    private int nextIndex;
    private DeviceTask currentDeviceTask;

    private MemoryDeviceTaskRunCursor(
        List<DeviceTask> deviceTasks, List<DeviceEntry> deviceEntries) {
      this.deviceTasks = deviceTasks;
      this.deviceEntries = deviceEntries;
      advance();
    }

    @Override
    public void advance() {
      if (nextIndex >= deviceTasks.size()) {
        currentDeviceTask = null;
        return;
      }
      currentDeviceTask = deviceTasks.get(nextIndex++);
    }

    @Override
    public boolean hasCurrentDeviceTask() {
      return currentDeviceTask != null;
    }

    @Override
    public DeviceTask getCurrentDeviceTask() {
      return currentDeviceTask;
    }

    @Override
    public DeviceEntry getCurrentDeviceEntry() {
      return deviceEntries.get(currentDeviceTask.deviceEntryIndex);
    }

    @Override
    public void close() {
      currentDeviceTask = null;
    }
  }

  private class DeviceCollector implements Closeable {

    private final Map<Integer, LazyTsFileDeviceIterator> deviceIteratorMap = new HashMap<>();

    private IDeviceID currentDevice;
    private List<DeviceOffset> currentDeviceOffsets;

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
                  entry.getKey(),
                  deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0],
                  deviceIterator.getCurrentDeviceMeasurementNodeOffset()[1]));
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

  private static class DeviceTask implements Accountable {

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(DeviceTask.class);

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
        ReadWriteIOUtils.write(offset.startOffset, outputStream);
        ReadWriteIOUtils.write(offset.endOffset, outputStream);
      }
    }

    private static DeviceTask deserialize(DataInputStream inputStream) throws IOException {
      int deviceEntryIndex = ReadWriteIOUtils.readInt(inputStream);
      int offsetSize = ReadWriteIOUtils.readInt(inputStream);
      List<DeviceOffset> offsets = new ArrayList<>(offsetSize);
      for (int i = 0; i < offsetSize; i++) {
        int fileIndex = ReadWriteIOUtils.readInt(inputStream);
        long startOffset = inputStream.readLong();
        long endOffset = inputStream.readLong();
        offsets.add(new DeviceOffset(fileIndex, startOffset, endOffset));
      }
      return new DeviceTask(deviceEntryIndex, offsets);
    }

    @Override
    public long ramBytesUsed() {
      return INSTANCE_SIZE
          + MemoryEstimationHelper.ARRAY_LIST_INSTANCE_SIZE
          + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
          + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * deviceOffsets.size()
          + deviceOffsets.size() * DeviceOffset.INSTANCE_SIZE;
    }
  }

  public static class DeviceOffset {

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(DeviceOffset.class);

    private final int fileIndex;
    private final long startOffset;
    private final long endOffset;

    private DeviceOffset(int fileIndex, long startOffset, long endOffset) {
      this.fileIndex = fileIndex;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    public int getFileIndex() {
      return fileIndex;
    }

    public long getStartOffset() {
      return startOffset;
    }

    public long getEndOffset() {
      return endOffset;
    }
  }
}
