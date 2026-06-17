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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
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
import java.util.Queue;

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

  private static final long TSFILE_READER_MEMORY_RESERVE_SIZE_IN_BYTES = 4L * 1024;

  private final MPPQueryContext queryContext;
  private final QueryId queryId;
  // This resource outlives the frontend planning phase, whose MPPQueryContext memory manager is
  // released after dispatch. Keep a dedicated manager and release it when this resource closes.
  private final MemoryReservationManager externalTsFileResourceMemoryReservationManager;
  private final Path queryTempRoot;
  private final String tableName;
  private final List<String> tsFilePaths;
  private final Map<Symbol, ColumnSchema> tableColumnSchema;
  private final List<TsFileResource> sharedTsFileResources;
  private final List<DeviceEntry> sharedDeviceEntries = new ArrayList<>();
  private final List<DeviceTaskPartition> deviceTaskPartitions = new ArrayList<>();
  private Comparator<DeviceEntry> deviceEntryComparator;
  private int deviceTaskPartitionCount;

  private volatile boolean closed;

  public ExternalTsFileQueryResource(
      MPPQueryContext queryContext,
      Path tempRoot,
      String tableName,
      List<String> tsFilePaths,
      Map<Symbol, ColumnSchema> tableColumnSchema) {
    this.queryContext = requireNonNull(queryContext, "queryContext is null");
    this.queryId = queryContext.getQueryId();
    this.externalTsFileResourceMemoryReservationManager =
        new NotThreadSafeMemoryReservationManager(
            queryId, ExternalTsFileQueryResource.class.getName());
    this.queryTempRoot = requireNonNull(tempRoot, "tempRoot is null");
    this.tableName = tableName;
    this.tsFilePaths = requireNonNull(tsFilePaths, "tsFilePaths");
    this.tableColumnSchema = tableColumnSchema;
    this.sharedTsFileResources = createTsFileResources(this.tsFilePaths);
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().increaseExternalFileReaderReference(tsFilePath);
    }
  }

  public void collectDeviceEntries(
      SchemaFilter schemaFilter, Comparator<DeviceEntry> comparator, int partitionCount) {
    checkNotClosed();
    if (partitionCount <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXTERNAL_TSFILE_DEVICE_TASK_PARTITION_COUNT_MUST_BE_POSITIVE);
    }
    this.deviceTaskPartitionCount = partitionCount;
    this.deviceEntryComparator = comparator;
    acquireMemoryForTsFileReaders();
    ExternalTsFileDeviceFilterVisitor deviceFilterVisitor = new ExternalTsFileDeviceFilterVisitor();
    try (DeviceCollector deviceCollector = new DeviceCollector()) {
      while (deviceCollector.hasNextDevice()) {
        queryContext.checkTimeOut();
        IDeviceID deviceID = deviceCollector.nextDevice();
        if (schemaFilter != null
            && !Boolean.TRUE.equals(schemaFilter.accept(deviceFilterVisitor, deviceID))) {
          continue;
        }
        DeviceEntry deviceEntry = new AlignedDeviceEntry(deviceID, new Binary[0]);
        int deviceEntryIndex = sharedDeviceEntries.size();
        externalTsFileResourceMemoryReservationManager.reserveMemoryCumulatively(
            deviceEntry.ramBytesUsed());
        sharedDeviceEntries.add(deviceEntry);
        DeviceTask deviceTask =
            new DeviceTask(deviceEntryIndex, deviceCollector.getCurrentDeviceOffsets());
        DeviceTaskPartition partition =
            getOrCreateDeviceTaskPartition(
                Math.floorMod(deviceID.hashCode(), deviceTaskPartitionCount));
        partition.add(deviceTask);
        if (partition.shouldFlush()) {
          partition.flush();
        }
      }
      collectDeviceTaskPartitions();
    }
  }

  private void acquireMemoryForTsFileReaders() {
    externalTsFileResourceMemoryReservationManager.reserveMemoryImmediately(
        tsFilePaths.size() * TSFILE_READER_MEMORY_RESERVE_SIZE_IN_BYTES);
  }

  public DeviceTaskRunReader getDeviceTaskRunReader(int partitionIndex) {
    checkNotClosed();
    DeviceTaskPartition partition = getDeviceTaskPartition(partitionIndex);
    try {
      return deviceEntryComparator == null
          ? new SequentialDeviceTaskRunReader(partition)
          : new PriorityDeviceTaskRunReader(partition);
    } catch (IOException e) {
      throw new RuntimeException(
          DataNodeQueryMessages.FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_TASK_RUN_READER, e);
    }
  }

  @TestOnly
  public void setDeviceEntryComparator(Comparator<DeviceEntry> deviceEntryComparator) {
    this.deviceEntryComparator = deviceEntryComparator;
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  public Map<Symbol, ColumnSchema> getTableColumnSchema() {
    return tableColumnSchema;
  }

  public List<TsFileResource> getSharedTsFileResources() {
    return sharedTsFileResources;
  }

  public List<DeviceEntry> getSharedDeviceEntries() {
    return sharedDeviceEntries;
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
        DataNodeQueryMessages.UNKNOWN_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION + partitionIndex);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    try {
      releaseFileReaderReferences();

      if (Files.exists(queryTempRoot)) {
        FileUtils.deleteFileOrDirectory(queryTempRoot.toFile(), true);
      }
    } finally {
      externalTsFileResourceMemoryReservationManager.releaseAllReservedMemory();
    }
  }

  private void releaseFileReaderReferences() {
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().decreaseExternalFileReaderReference(tsFilePath);
    }
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_BEEN_CLOSED + queryId);
    }
  }

  private static List<TsFileResource> createTsFileResources(List<String> tsFilePaths) {
    List<TsFileResource> tsFileResources = new ArrayList<>(tsFilePaths.size());
    for (String tsFilePath : tsFilePaths) {
      TsFileResource resource =
          new TsFileResource(new File(tsFilePath), TsFileResourceStatus.NORMAL);
      resource.setTimeIndex(new FileTimeIndex(Long.MIN_VALUE, Long.MAX_VALUE));
      tsFileResources.add(resource);
    }
    return Collections.unmodifiableList(tsFileResources);
  }

  public class DeviceTaskPartition {

    private static final long DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES = 8L * 1024 * 1024;
    private static final long MEMORY_RESERVE_BATCH_SIZE_IN_BYTES = 1024 * 1024;

    private final int partitionIndex;
    private final PlanNodeId planNodeId;
    private final List<DeviceTask> pendingDeviceTasks = new ArrayList<>();
    private final List<Integer> deviceEntryIndexes = new ArrayList<>();
    private final List<Path> runFiles = new ArrayList<>();
    private long reservedBytes;
    private long unreservedBytes;

    DeviceTaskPartition(int partitionIndex) {
      this.partitionIndex = partitionIndex;
      this.planNodeId = queryId.genPlanNodeId();
    }

    public int getPartitionIndex() {
      return partitionIndex;
    }

    public PlanNodeId getPlanNodeId() {
      return planNodeId;
    }

    public List<Integer> getDeviceEntryIndexes() {
      return deviceEntryIndexes;
    }

    void add(DeviceTask deviceTask) {
      pendingDeviceTasks.add(deviceTask);
      unreservedBytes += deviceTask.ramBytesUsed();
    }

    void flush() {
      if (pendingDeviceTasks.isEmpty()) {
        return;
      }
      sortPendingDeviceTasks();
      try {
        runFiles.add(
            writeDeviceTaskRun(
                queryTempRoot.resolve(planNodeId.getId()), runFiles.size(), pendingDeviceTasks));
      } catch (IOException e) {
        throw new RuntimeException(
            DataNodeQueryMessages.FAILED_TO_FLUSH_EXTERNAL_TSFILE_DEVICE_TASK_PARTITION, e);
      }
      for (DeviceTask deviceTask : pendingDeviceTasks) {
        deviceEntryIndexes.add(deviceTask.deviceEntryIndex);
      }
      pendingDeviceTasks.clear();
      releaseDeviceTaskMemory();
    }

    private void sortPendingDeviceTasks() {
      if (deviceEntryComparator != null) {
        pendingDeviceTasks.sort(
            (left, right) ->
                compareDeviceEntryIndexes(left.deviceEntryIndex, right.deviceEntryIndex));
      } else {
        pendingDeviceTasks.sort(
            (left, right) ->
                sharedDeviceEntries
                    .get(left.deviceEntryIndex)
                    .getDeviceID()
                    .compareTo(sharedDeviceEntries.get(right.deviceEntryIndex).getDeviceID()));
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
      if (unreservedBytes == 0) {
        return true;
      }
      try {
        externalTsFileResourceMemoryReservationManager.reserveMemoryImmediately(unreservedBytes);
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
        externalTsFileResourceMemoryReservationManager.releaseMemoryCumulatively(reservedBytes);
        reservedBytes = 0;
      }
      unreservedBytes = 0;
    }

    void finish() {
      if (pendingDeviceTasks.isEmpty()) {
        return;
      }
      if (!reserveUnreservedMemory()) {
        flush();
        return;
      }
      sortPendingDeviceTasks();
      for (DeviceTask deviceTask : pendingDeviceTasks) {
        deviceEntryIndexes.add(deviceTask.deviceEntryIndex);
      }
    }

    private void sortDeviceEntries() {
      if (deviceEntryComparator != null) {
        deviceEntryIndexes.sort(ExternalTsFileQueryResource.this::compareDeviceEntryIndexes);
      } else {
        deviceEntryIndexes.sort(
            (left, right) ->
                sharedDeviceEntries
                    .get(left)
                    .getDeviceID()
                    .compareTo(sharedDeviceEntries.get(right).getDeviceID()));
      }
    }

    private List<Path> getRunFiles() {
      return runFiles;
    }

    private List<DeviceTask> getPendingDeviceTasks() {
      return pendingDeviceTasks;
    }
  }

  private int compareDeviceEntryIndexes(int leftIndex, int rightIndex) {
    int result =
        deviceEntryComparator.compare(
            sharedDeviceEntries.get(leftIndex), sharedDeviceEntries.get(rightIndex));
    // Use the stable device entry index as a tie-breaker so list sorting and run-file merging keep
    // the same deterministic order when the pushed-down comparator is not a total order.
    return result != 0 ? result : Integer.compare(leftIndex, rightIndex);
  }

  private DeviceTaskPartition getOrCreateDeviceTaskPartition(int partitionIndex) {
    for (DeviceTaskPartition partition : deviceTaskPartitions) {
      if (partition.getPartitionIndex() == partitionIndex) {
        return partition;
      }
    }
    DeviceTaskPartition partition = new DeviceTaskPartition(partitionIndex);
    deviceTaskPartitions.add(partition);
    return partition;
  }

  private void collectDeviceTaskPartitions() {
    deviceTaskPartitions.sort(Comparator.comparingInt(DeviceTaskPartition::getPartitionIndex));
    for (DeviceTaskPartition partition : deviceTaskPartitions) {
      partition.finish();
      partition.sortDeviceEntries();
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

  public abstract class DeviceTaskRunReader implements AutoCloseable {

    private DeviceEntry currentDevice;
    private QueryDataSource currentDeviceQueryDataSource;
    private Map<TsFileResource, DeviceOffset> currentDeviceOffsetMap;

    protected void initialize(DeviceTaskPartition partition) throws IOException {
      for (Path runFile : partition.getRunFiles()) {
        DeviceTaskRunCursor cursor = new DiskDeviceTaskRunCursor(runFile);
        if (cursor.hasCurrentDeviceTask()) {
          addCursor(cursor);
        } else {
          cursor.close();
        }
      }
      DeviceTaskRunCursor memoryCursor =
          new MemoryDeviceTaskRunCursor(partition.getPendingDeviceTasks());
      if (memoryCursor.hasCurrentDeviceTask()) {
        addCursor(memoryCursor);
      } else {
        memoryCursor.close();
      }
    }

    public boolean nextDevice() throws IOException {
      DeviceTaskRunCursor cursor = pollCursor();
      if (cursor == null) {
        return false;
      }
      DeviceTask result = cursor.getCurrentDeviceTask();
      cursor.advance();
      recycleOrCloseCursor(cursor);

      currentDevice = sharedDeviceEntries.get(result.deviceEntryIndex);
      List<TsFileResource> unseqResources = new ArrayList<>(result.deviceOffsets.size());
      currentDeviceOffsetMap = new HashMap<>(result.deviceOffsets.size());
      for (DeviceOffset deviceOffset : result.deviceOffsets) {
        TsFileResource tsFileResource = sharedTsFileResources.get(deviceOffset.getFileIndex());
        unseqResources.add(tsFileResource);
        currentDeviceOffsetMap.put(tsFileResource, deviceOffset);
      }
      currentDeviceQueryDataSource = new QueryDataSource(Collections.emptyList(), unseqResources);
      currentDeviceQueryDataSource.setSingleDevice(true);
      return true;
    }

    protected abstract DeviceTaskRunCursor pollCursor() throws IOException;

    protected abstract void addCursor(DeviceTaskRunCursor cursor);

    protected abstract void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException;

    protected abstract DeviceTaskRunCursor pollRemainingCursor();

    public DeviceEntry getCurrentDevice() {
      return currentDevice;
    }

    public QueryDataSource getCurrentDeviceQueryDataSource() {
      return currentDeviceQueryDataSource;
    }

    public Map<TsFileResource, DeviceOffset> getCurrentDeviceOffsetMap() {
      return currentDeviceOffsetMap;
    }

    @Override
    public void close() throws IOException {
      IOException exception = null;
      DeviceTaskRunCursor cursor;
      while ((cursor = pollRemainingCursor()) != null) {
        try {
          cursor.close();
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

  private class PriorityDeviceTaskRunReader extends DeviceTaskRunReader {

    private final Queue<DeviceTaskRunCursor> runCursors =
        new PriorityQueue<>(
            (left, right) ->
                compareDeviceEntryIndexes(
                    left.getCurrentDeviceTask().deviceEntryIndex,
                    right.getCurrentDeviceTask().deviceEntryIndex));

    private PriorityDeviceTaskRunReader(DeviceTaskPartition partition) throws IOException {
      initialize(partition);
    }

    @Override
    protected DeviceTaskRunCursor pollCursor() {
      return runCursors.poll();
    }

    @Override
    protected void addCursor(DeviceTaskRunCursor cursor) {
      runCursors.add(cursor);
    }

    @Override
    protected void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException {
      if (cursor.hasCurrentDeviceTask()) {
        addCursor(cursor);
      } else {
        cursor.close();
      }
    }

    @Override
    protected DeviceTaskRunCursor pollRemainingCursor() {
      return runCursors.poll();
    }
  }

  private class SequentialDeviceTaskRunReader extends DeviceTaskRunReader {

    private final Iterator<Path> runFileIterator;
    private final List<DeviceTask> pendingDeviceTasks;
    private DeviceTaskRunCursor currentCursor;
    private boolean memoryCursorLoaded;

    private SequentialDeviceTaskRunReader(DeviceTaskPartition partition) {
      this.runFileIterator = partition.getRunFiles().iterator();
      this.pendingDeviceTasks = partition.getPendingDeviceTasks();
    }

    @Override
    protected DeviceTaskRunCursor pollCursor() throws IOException {
      if (currentCursor != null) {
        DeviceTaskRunCursor cursor = currentCursor;
        currentCursor = null;
        return cursor;
      }
      while (runFileIterator.hasNext()) {
        DeviceTaskRunCursor cursor = new DiskDeviceTaskRunCursor(runFileIterator.next());
        if (cursor.hasCurrentDeviceTask()) {
          return cursor;
        }
        cursor.close();
      }
      if (!memoryCursorLoaded) {
        memoryCursorLoaded = true;
        DeviceTaskRunCursor cursor = new MemoryDeviceTaskRunCursor(pendingDeviceTasks);
        if (cursor.hasCurrentDeviceTask()) {
          return cursor;
        }
        cursor.close();
      }
      return null;
    }

    @Override
    protected void addCursor(DeviceTaskRunCursor cursor) {
      currentCursor = cursor;
    }

    @Override
    protected void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException {
      if (cursor.hasCurrentDeviceTask()) {
        currentCursor = cursor;
      } else {
        cursor.close();
      }
    }

    @Override
    protected DeviceTaskRunCursor pollRemainingCursor() {
      DeviceTaskRunCursor cursor = currentCursor;
      currentCursor = null;
      return cursor;
    }
  }

  private interface DeviceTaskRunCursor extends Closeable {

    boolean hasCurrentDeviceTask();

    DeviceTask getCurrentDeviceTask();

    void advance() throws IOException;
  }

  private static class DiskDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final DataInputStream inputStream;
    private int remainingDeviceTasks;
    private DeviceTask currentDeviceTask;

    private DiskDeviceTaskRunCursor(Path runFile) throws IOException {
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
    public void close() throws IOException {
      inputStream.close();
    }
  }

  private static class MemoryDeviceTaskRunCursor implements DeviceTaskRunCursor {

    private final List<DeviceTask> deviceTasks;
    private int nextIndex;
    private DeviceTask currentDeviceTask;

    private MemoryDeviceTaskRunCursor(List<DeviceTask> deviceTasks) {
      this.deviceTasks = deviceTasks;
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
                  .get(tsFilePaths.get(fileIndex), null, true, null, true);
          deviceIteratorMap.put(fileIndex, new LazyTsFileDeviceIterator(reader, tableName, null));
        }
      } catch (IOException e) {
        close();
        throw new RuntimeException(
            DataNodeQueryMessages.FAILED_TO_CREATE_EXTERNAL_TSFILE_DEVICE_COLLECTOR, e);
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

  static class DeviceTask implements Accountable {

    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(DeviceTask.class);

    private final int deviceEntryIndex;
    private final List<DeviceOffset> deviceOffsets;

    DeviceTask(int deviceEntryIndex, List<DeviceOffset> deviceOffsets) {
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
          + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class)
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

    DeviceOffset(int fileIndex, long startOffset, long endOffset) {
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
