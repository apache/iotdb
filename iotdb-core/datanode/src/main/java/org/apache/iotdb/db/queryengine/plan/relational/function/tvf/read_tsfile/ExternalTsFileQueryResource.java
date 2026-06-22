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
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedOutputStream;
import java.io.Closeable;
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
public class ExternalTsFileQueryResource {

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

  // Counts FragmentInstances that have entered runtime datasource initialization and may read
  // files under queryTempRoot. QueryExecution cleanup only closes this resource when the count is
  // zero. Otherwise the last FragmentInstance release closes it, which prevents QueryExecution from
  // deleting temporary run files while drivers are still reading them.
  private int fragmentInstanceUsageCount;
  private boolean closed;

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
    if (partitionCount <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXTERNAL_TSFILE_DEVICE_TASK_PARTITION_COUNT_MUST_BE_POSITIVE);
    }
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
        // Reserve the DeviceEntry reference in sharedDeviceEntries, plus the boxed index and its
        // reference that partition.add will append to deviceEntryIndexes.
        externalTsFileResourceMemoryReservationManager.reserveMemoryCumulatively(
            deviceEntry.ramBytesUsed()
                + MemoryEstimationHelper.INTEGER_INSTANCE_SIZE
                + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        sharedDeviceEntries.add(deviceEntry);
        ExternalTsFileDeviceQueryTask deviceTask =
            new ExternalTsFileDeviceQueryTask(
                deviceEntryIndex, deviceCollector.getCurrentDeviceOffsets());
        DeviceTaskPartition partition =
            getOrCreateDeviceTaskPartition(Math.floorMod(deviceID.hashCode(), partitionCount));
        partition.add(deviceTask);
        if (partition.shouldFlush()) {
          partition.flush();
        }
      }
      sealDeviceTaskPartitions();
    }
  }

  private void acquireMemoryForTsFileReaders() {
    externalTsFileResourceMemoryReservationManager.reserveMemoryImmediately(
        tsFilePaths.size() * TSFILE_READER_MEMORY_RESERVE_SIZE_IN_BYTES);
  }

  public DeviceTaskRunReader getDeviceTaskRunReader(int partitionIndex) {
    DeviceTaskPartition partition = getDeviceTaskPartition(partitionIndex);
    try {
      return new DeviceTaskRunReader(
          deviceEntryComparator == null
              ? new SequentialDeviceTaskRunCursorManager(partition)
              : new PriorityDeviceTaskRunCursorManager(partition));
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

  public synchronized void retainFragmentInstanceUsage() {
    if (closed) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXTERNAL_TSFILE_QUERY_RESOURCE_HAS_BEEN_CLOSED + queryId);
    }
    fragmentInstanceUsageCount++;
  }

  public synchronized void closeByFragmentInstance() {
    fragmentInstanceUsageCount--;
    if (fragmentInstanceUsageCount < 0) {
      fragmentInstanceUsageCount++;
      throw new IllegalStateException(
          DataNodeQueryMessages.EXTERNAL_TSFILE_FRAGMENT_INSTANCE_USAGE_COUNT_CANNOT_BE_NEGATIVE);
    }
    if (fragmentInstanceUsageCount == 0) {
      close();
    }
  }

  public synchronized void closeByQueryExecution() {
    if (fragmentInstanceUsageCount == 0) {
      close();
    }
  }

  private void close() {
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
      try {
        externalTsFileResourceMemoryReservationManager.releaseAllReservedMemory();
      } finally {
        queryContext.removeExternalTsFileQueryResource(this);
      }
    }
  }

  private void releaseFileReaderReferences() {
    for (String tsFilePath : tsFilePaths) {
      FileReaderManager.getInstance().decreaseExternalFileReaderReference(tsFilePath);
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
    private final List<ExternalTsFileDeviceQueryTask> pendingDeviceTasks = new ArrayList<>();
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

    void add(ExternalTsFileDeviceQueryTask deviceTask) {
      deviceEntryIndexes.add(deviceTask.deviceEntryIndex());
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
      pendingDeviceTasks.clear();
      releaseDeviceTaskMemory();
    }

    private void sortPendingDeviceTasks() {
      if (deviceEntryComparator == null) {
        return;
      }
      pendingDeviceTasks.sort(
          (left, right) ->
              compareDeviceEntryIndexes(left.deviceEntryIndex(), right.deviceEntryIndex()));
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
      if (deviceEntryComparator != null) {
        deviceEntryIndexes.sort(ExternalTsFileQueryResource.this::compareDeviceEntryIndexes);
      }

      if (pendingDeviceTasks.isEmpty()) {
        return;
      }
      if (!reserveUnreservedMemory()) {
        flush();
      } else {
        sortPendingDeviceTasks();
      }
    }

    private List<Path> getRunFiles() {
      return runFiles;
    }

    private List<ExternalTsFileDeviceQueryTask> getPendingDeviceTasks() {
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

  private void sealDeviceTaskPartitions() {
    deviceTaskPartitions.sort(Comparator.comparingInt(DeviceTaskPartition::getPartitionIndex));
    for (DeviceTaskPartition partition : deviceTaskPartitions) {
      partition.finish();
    }
  }

  private Path writeDeviceTaskRun(
      Path runRoot, int runIndex, List<ExternalTsFileDeviceQueryTask> deviceTasks)
      throws IOException {
    Files.createDirectories(runRoot);
    Path runFile = runRoot.resolve("run-" + runIndex + ".bin");
    try (DataOutputStream outputStream =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(runFile)))) {
      ReadWriteIOUtils.write(deviceTasks.size(), outputStream);
      for (ExternalTsFileDeviceQueryTask deviceTask : deviceTasks) {
        deviceTask.serialize(outputStream);
      }
    }
    return runFile;
  }

  public class DeviceTaskRunReader implements AutoCloseable {

    private final DeviceTaskRunCursorManager cursorManager;
    private DeviceEntry currentDevice;
    private QueryDataSource currentDeviceQueryDataSource;
    private Map<TsFileResource, ExternalTsFileDeviceQueryTask.DeviceOffset> currentDeviceOffsetMap;

    private DeviceTaskRunReader(DeviceTaskRunCursorManager cursorManager) {
      this.cursorManager = cursorManager;
    }

    public boolean nextDevice() throws IOException {
      DeviceTaskRunCursor cursor = cursorManager.pollCursor();
      if (cursor == null) {
        return false;
      }
      ExternalTsFileDeviceQueryTask result = cursor.getCurrentDeviceTask();
      try {
        cursor.advance();
        cursorManager.recycleOrCloseCursor(cursor);
        cursor = null;
      } finally {
        if (cursor != null) {
          cursor.close();
        }
      }

      currentDevice = sharedDeviceEntries.get(result.deviceEntryIndex());
      List<TsFileResource> unseqResources = new ArrayList<>(result.deviceOffsets().size());
      currentDeviceOffsetMap = new HashMap<>(result.deviceOffsets().size());
      for (ExternalTsFileDeviceQueryTask.DeviceOffset deviceOffset : result.deviceOffsets()) {
        TsFileResource tsFileResource = sharedTsFileResources.get(deviceOffset.getFileIndex());
        unseqResources.add(tsFileResource);
        currentDeviceOffsetMap.put(tsFileResource, deviceOffset);
      }
      currentDeviceQueryDataSource = new QueryDataSource(Collections.emptyList(), unseqResources);
      currentDeviceQueryDataSource.setSingleDevice(true);
      return true;
    }

    public DeviceEntry getCurrentDevice() {
      return currentDevice;
    }

    public QueryDataSource getCurrentDeviceQueryDataSource() {
      return currentDeviceQueryDataSource;
    }

    public Map<TsFileResource, ExternalTsFileDeviceQueryTask.DeviceOffset>
        getCurrentDeviceOffsetMap() {
      return currentDeviceOffsetMap;
    }

    @Override
    public void close() throws IOException {
      IOException exception = null;
      DeviceTaskRunCursor cursor;
      while ((cursor = cursorManager.pollRemainingCursor()) != null) {
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

  private interface DeviceTaskRunCursorManager {

    DeviceTaskRunCursor pollCursor() throws IOException;

    void addCursor(DeviceTaskRunCursor cursor);

    void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException;

    DeviceTaskRunCursor pollRemainingCursor();
  }

  private class PriorityDeviceTaskRunCursorManager implements DeviceTaskRunCursorManager {

    private final Queue<DeviceTaskRunCursor> runCursors =
        new PriorityQueue<>(
            (left, right) ->
                compareDeviceEntryIndexes(
                    left.getCurrentDeviceTask().deviceEntryIndex(),
                    right.getCurrentDeviceTask().deviceEntryIndex()));

    private PriorityDeviceTaskRunCursorManager(DeviceTaskPartition partition) throws IOException {
      initialize(partition);
    }

    private void addIfNotEmpty(DeviceTaskRunCursor cursor) throws IOException {
      if (cursor.hasCurrentDeviceTask()) {
        addCursor(cursor);
      } else {
        cursor.close();
      }
    }

    private void initialize(DeviceTaskPartition partition) throws IOException {
      try {
        for (Path runFile : partition.getRunFiles()) {
          addIfNotEmpty(new DeviceTaskRunCursor.DiskDeviceTaskRunCursor(runFile));
        }
        addIfNotEmpty(
            new DeviceTaskRunCursor.MemoryDeviceTaskRunCursor(partition.getPendingDeviceTasks()));
      } catch (IOException | RuntimeException e) {
        closeAllCursorsOnInitializationFailure(e);
        throw e;
      }
    }

    private void closeAllCursorsOnInitializationFailure(final Exception exception) {
      DeviceTaskRunCursor cursor;
      while ((cursor = pollRemainingCursor()) != null) {
        try {
          cursor.close();
        } catch (IOException e) {
          exception.addSuppressed(e);
        }
      }
    }

    @Override
    public DeviceTaskRunCursor pollCursor() {
      return runCursors.poll();
    }

    @Override
    public void addCursor(DeviceTaskRunCursor cursor) {
      runCursors.add(cursor);
    }

    @Override
    public void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException {
      if (cursor.hasCurrentDeviceTask()) {
        addCursor(cursor);
      } else {
        cursor.close();
      }
    }

    @Override
    public DeviceTaskRunCursor pollRemainingCursor() {
      return runCursors.poll();
    }
  }

  private class SequentialDeviceTaskRunCursorManager implements DeviceTaskRunCursorManager {

    private final Iterator<Path> runFileIterator;
    private final List<ExternalTsFileDeviceQueryTask> pendingDeviceTasks;
    private DeviceTaskRunCursor currentCursor;
    private boolean memoryCursorLoaded;

    private SequentialDeviceTaskRunCursorManager(DeviceTaskPartition partition) {
      this.runFileIterator = partition.getRunFiles().iterator();
      this.pendingDeviceTasks = partition.getPendingDeviceTasks();
    }

    @Override
    public DeviceTaskRunCursor pollCursor() throws IOException {
      if (currentCursor != null) {
        DeviceTaskRunCursor cursor = currentCursor;
        currentCursor = null;
        return cursor;
      }
      while (runFileIterator.hasNext()) {
        DeviceTaskRunCursor cursor =
            new DeviceTaskRunCursor.DiskDeviceTaskRunCursor(runFileIterator.next());
        if (cursor.hasCurrentDeviceTask()) {
          return cursor;
        }
        cursor.close();
      }
      if (!memoryCursorLoaded) {
        memoryCursorLoaded = true;
        DeviceTaskRunCursor cursor =
            new DeviceTaskRunCursor.MemoryDeviceTaskRunCursor(pendingDeviceTasks);
        if (cursor.hasCurrentDeviceTask()) {
          return cursor;
        }
        cursor.close();
      }
      return null;
    }

    @Override
    public void addCursor(DeviceTaskRunCursor cursor) {
      currentCursor = cursor;
    }

    @Override
    public void recycleOrCloseCursor(DeviceTaskRunCursor cursor) throws IOException {
      if (cursor.hasCurrentDeviceTask()) {
        addCursor(cursor);
      } else {
        cursor.close();
      }
    }

    @Override
    public DeviceTaskRunCursor pollRemainingCursor() {
      DeviceTaskRunCursor cursor = currentCursor;
      currentCursor = null;
      return cursor;
    }
  }

  private class DeviceCollector implements Closeable {

    private final Map<Integer, LazyTsFileDeviceIterator> deviceIteratorMap = new HashMap<>();

    private IDeviceID currentDevice;
    private List<ExternalTsFileDeviceQueryTask.DeviceOffset> currentDeviceOffsets;

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
      List<ExternalTsFileDeviceQueryTask.DeviceOffset> deviceOffsets = new ArrayList<>();
      for (Map.Entry<Integer, LazyTsFileDeviceIterator> entry : deviceIteratorMap.entrySet()) {
        LazyTsFileDeviceIterator deviceIterator = entry.getValue();
        if (currentDevice != null
            && deviceIterator.hasCurrent()
            && currentDevice.equals(deviceIterator.getCurrentDeviceID())) {
          deviceOffsets.add(
              new ExternalTsFileDeviceQueryTask.DeviceOffset(
                  entry.getKey(),
                  deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0],
                  deviceIterator.getCurrentDeviceMeasurementNodeOffset()[1]));
        }
      }
      currentDeviceOffsets = deviceOffsets;
    }

    private List<ExternalTsFileDeviceQueryTask.DeviceOffset> getCurrentDeviceOffsets() {
      return currentDeviceOffsets;
    }

    @Override
    public void close() {
      deviceIteratorMap.clear();
      currentDeviceOffsets = Collections.emptyList();
    }
  }
}
