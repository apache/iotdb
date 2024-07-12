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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.ResourceByPathUtils;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.flush.CloseFileListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.DiskAlignedChunkHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.DiskChunkHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.UnclosedFileScanHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.SharedTimeDataBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.GET_QUERY_RESOURCE_FROM_MEM;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.FLUSHING_MEMTABLE;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.WORKING_MEMTABLE;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileProcessor {

  /** Logger fot this class. */
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);

  /** Storage group name of this tsfile. */
  private final String storageGroupName;

  /** IoTDB config. */
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** Database info for mem control. */
  private final DataRegionInfo dataRegionInfo;

  /** Tsfile processor info for mem control. */
  private TsFileProcessorInfo tsFileProcessorInfo;

  /** Sync this object in read() and asyncTryToFlush(). */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  /** Modification to memtable mapping. */
  private final List<Pair<Modification, IMemTable>> modsToMemtable = new ArrayList<>();

  /** Writer for restore tsfile and flushing. */
  private RestorableTsFileIOWriter writer;

  /** Tsfile resource for index this tsfile. */
  private final TsFileResource tsFileResource;

  /** Time range index to indicate this processor belongs to which time range */
  private long timeRangeId;

  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;

  /** A lock to mutual exclude read and read */
  private final ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();

  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;

  /** Working memtable. */
  private IMemTable workMemTable;

  /** Last flush time to flush the working memtable. */
  private long lastWorkMemtableFlushTime;

  /** This callback is called before the workMemtable is added into the flushingMemTables. */
  private final DataRegion.UpdateEndTimeCallBack updateLatestFlushTimeCallback;

  public static final long FLUSH_POINT_COUNT_NOT_SET = -1;

  /** Point count when the memtable is flushed. Used for metrics on PipeConsensus' receiver side. */
  private long memTableFlushPointCount = FLUSH_POINT_COUNT_NOT_SET;

  /** Wal node. */
  private final IWALNode walNode;

  /** Whether it's a sequence file or not. */
  private final boolean sequence;

  /** Total memtable size for mem control. */
  private long totalMemTableSize;

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE =
      "{}: {} get flushQueryLock write lock released";

  /** Close file listener. */
  private final List<CloseFileListener> closeFileListeners = new CopyOnWriteArrayList<>();

  /** Flush file listener. */
  private final List<FlushListener> flushListeners = new ArrayList<>();

  private final QueryExecutionMetricSet QUERY_EXECUTION_METRICS =
      QueryExecutionMetricSet.getInstance();
  private final QueryResourceMetricSet QUERY_RESOURCE_METRICS =
      QueryResourceMetricSet.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  public static final int MEMTABLE_NOT_EXIST = -1;

  @SuppressWarnings("squid:S107")
  public TsFileProcessor(
      String storageGroupName,
      File tsfile,
      DataRegionInfo dataRegionInfo,
      CloseFileListener closeTsFileCallback,
      DataRegion.UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    // this.sequence should be assigned at first because `this` will be passed as parameter to other
    // val later
    this.sequence = sequence;
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.dataRegionInfo = dataRegionInfo;
    this.writer = new RestorableTsFileIOWriter(tsfile);
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.walNode =
        WALManager.getInstance()
            .applyForWALNode(WALManager.getApplicantUniqueId(storageGroupName, sequence));
    flushListeners.add(FlushListener.DefaultMemTableFLushListener.INSTANCE);
    flushListeners.add(this.walNode);
    closeFileListeners.add(closeTsFileCallback);
    logger.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
  }

  @SuppressWarnings("java:S107") // ignore number of arguments
  public TsFileProcessor(
      String storageGroupName,
      DataRegionInfo dataRegionInfo,
      TsFileResource tsFileResource,
      CloseFileListener closeUnsealedTsFileProcessor,
      DataRegion.UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence,
      RestorableTsFileIOWriter writer) {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = tsFileResource;
    this.dataRegionInfo = dataRegionInfo;
    this.writer = writer;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    this.walNode =
        WALManager.getInstance()
            .applyForWALNode(WALManager.getApplicantUniqueId(storageGroupName, sequence));
    flushListeners.add(FlushListener.DefaultMemTableFLushListener.INSTANCE);
    flushListeners.add(this.walNode);
    closeFileListeners.add(closeUnsealedTsFileProcessor);
    logger.info("reopen a tsfile processor {}", tsFileResource.getTsFile());
  }

  /**
   * Insert data in an InsertRowNode into the workingMemtable.
   *
   * @param insertRowNode physical plan of insertion
   */
  public void insert(InsertRowNode insertRowNode, long[] costsForMetrics)
      throws WriteProcessException {

    if (workMemTable == null) {
      long startTime = System.nanoTime();
      createNewWorkingMemTable();
      // recordCreateMemtableBlockCost
      costsForMetrics[0] += System.nanoTime() - startTime;
      WritingMetrics.getInstance()
          .recordActiveMemTableCount(dataRegionInfo.getDataRegion().getDataRegionId(), 1);
    }

    long[] memIncrements;

    long memControlStartTime = System.nanoTime();
    if (insertRowNode.isAligned()) {
      memIncrements =
          checkAlignedMemCostAndAddToTspInfoForRow(
              insertRowNode.getDeviceID(), insertRowNode.getMeasurements(),
              insertRowNode.getDataTypes(), insertRowNode.getValues());
    } else {
      memIncrements =
          checkMemCostAndAddToTspInfoForRow(
              insertRowNode.getDeviceID(), insertRowNode.getMeasurements(),
              insertRowNode.getDataTypes(), insertRowNode.getValues());
    }
    // recordScheduleMemoryBlockCost
    costsForMetrics[1] += System.nanoTime() - memControlStartTime;

    long startTime = System.nanoTime();
    WALFlushListener walFlushListener;
    try {
      walFlushListener = walNode.log(workMemTable.getMemTableId(), insertRowNode);
      if (walFlushListener.waitForResult() == AbstractResultListener.Status.FAILURE) {
        throw walFlushListener.getCause();
      }
    } catch (Exception e) {
      rollbackMemoryInfo(memIncrements);
      throw new WriteProcessException(
          String.format(
              "%s: %s write WAL failed",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath()),
          e);
    } finally {
      // recordScheduleWalCost
      costsForMetrics[2] += System.nanoTime() - startTime;
    }

    startTime = System.nanoTime();

    PipeDataNodeAgent.runtime().assignSimpleProgressIndexIfNeeded(insertRowNode);
    if (!insertRowNode.isGeneratedByPipe()) {
      workMemTable.markAsNotGeneratedByPipe();
    }
    PipeInsertionDataNodeListener.getInstance()
        .listenToInsertNode(
            dataRegionInfo.getDataRegion().getDataRegionId(),
            walFlushListener.getWalEntryHandler(),
            insertRowNode,
            tsFileResource);

    if (insertRowNode.isAligned()) {
      workMemTable.insertAlignedRow(insertRowNode);
    } else {
      workMemTable.insert(insertRowNode);
    }

    // Update start time of this memtable
    tsFileResource.updateStartTime(insertRowNode.getDeviceID(), insertRowNode.getTime());
    // For sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // For unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(insertRowNode.getDeviceID(), insertRowNode.getTime());
    }

    tsFileResource.updateProgressIndex(insertRowNode.getProgressIndex());
    // RecordScheduleMemTableCost
    costsForMetrics[3] += System.nanoTime() - startTime;
  }

  public void insert(InsertRowsNode insertRowsNode, long[] costsForMetrics)
      throws WriteProcessException {

    if (workMemTable == null) {
      long startTime = System.nanoTime();
      createNewWorkingMemTable();
      // recordCreateMemtableBlockCost
      costsForMetrics[0] += System.nanoTime() - startTime;
      WritingMetrics.getInstance()
          .recordActiveMemTableCount(dataRegionInfo.getDataRegion().getDataRegionId(), 1);
    }

    long[] memIncrements;

    long memControlStartTime = System.nanoTime();
    if (insertRowsNode.isAligned()) {
      memIncrements = checkAlignedMemCostAndAddToTspInfoForRows(insertRowsNode);
    } else {
      memIncrements = checkMemCostAndAddToTspInfoForRows(insertRowsNode);
    }
    // recordScheduleMemoryBlockCost
    costsForMetrics[1] += System.nanoTime() - memControlStartTime;

    long startTime = System.nanoTime();
    WALFlushListener walFlushListener;
    try {
      walFlushListener = walNode.log(workMemTable.getMemTableId(), insertRowsNode);
      if (walFlushListener.waitForResult() == AbstractResultListener.Status.FAILURE) {
        throw walFlushListener.getCause();
      }
    } catch (Exception e) {
      rollbackMemoryInfo(memIncrements);
      throw new WriteProcessException(
          String.format(
              "%s: %s write WAL failed",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath()),
          e);
    } finally {
      // recordScheduleWalCost
      costsForMetrics[2] += System.nanoTime() - startTime;
    }

    startTime = System.nanoTime();

    PipeDataNodeAgent.runtime().assignSimpleProgressIndexIfNeeded(insertRowsNode);
    if (!insertRowsNode.isGeneratedByPipe()) {
      workMemTable.markAsNotGeneratedByPipe();
    }
    PipeInsertionDataNodeListener.getInstance()
        .listenToInsertNode(
            dataRegionInfo.getDataRegion().getDataRegionId(),
            walFlushListener.getWalEntryHandler(),
            insertRowsNode,
            tsFileResource);
    for (InsertRowNode insertRowNode : insertRowsNode.getInsertRowNodeList()) {

      if (insertRowNode.isAligned()) {
        workMemTable.insertAlignedRow(insertRowNode);
      } else {
        workMemTable.insert(insertRowNode);
      }

      // update start time of this memtable
      tsFileResource.updateStartTime(insertRowNode.getDeviceID(), insertRowNode.getTime());
      // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
      // for unsequence tsfile, we have to update the endTime for each insertion.
      if (!sequence) {
        tsFileResource.updateEndTime(insertRowNode.getDeviceID(), insertRowNode.getTime());
      }
    }
    tsFileResource.updateProgressIndex(insertRowsNode.getProgressIndex());
    // recordScheduleMemTableCost
    costsForMetrics[3] += System.nanoTime() - startTime;
  }

  private void createNewWorkingMemTable() {
    workMemTable =
        MemTableManager.getInstance()
            .getAvailableMemTable(
                dataRegionInfo.getDataRegion().getDatabaseName(),
                dataRegionInfo.getDataRegion().getDataRegionId());
    walNode.onMemTableCreated(workMemTable, tsFileResource.getTsFilePath());
  }

  /**
   * Insert batch data of insertTabletPlan into the workingMemtable. The rows to be inserted are in
   * the range [start, end). Null value in each column values will be replaced by the subsequent
   * non-null value, e.g., {1, null, 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletNode insert a tablet of a device
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   */
  public void insertTablet(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results)
      throws WriteProcessException {

    if (workMemTable == null) {
      long startTime = System.nanoTime();
      createNewWorkingMemTable();
      PERFORMANCE_OVERVIEW_METRICS.recordCreateMemtableBlockCost(System.nanoTime() - startTime);
      WritingMetrics.getInstance()
          .recordActiveMemTableCount(dataRegionInfo.getDataRegion().getDataRegionId(), 1);
    }

    long[] memIncrements;
    try {
      long startTime = System.nanoTime();
      if (insertTabletNode.isAligned()) {
        memIncrements =
            checkAlignedMemCostAndAddToTspForTablet(
                insertTabletNode.getDeviceID(),
                insertTabletNode.getMeasurements(),
                insertTabletNode.getDataTypes(),
                insertTabletNode.getColumns(),
                start,
                end);
      } else {
        memIncrements =
            checkMemCostAndAddToTspInfoForTablet(
                insertTabletNode.getDeviceID(),
                insertTabletNode.getMeasurements(),
                insertTabletNode.getDataTypes(),
                insertTabletNode.getColumns(),
                start,
                end);
      }
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(System.nanoTime() - startTime);
    } catch (WriteProcessException e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, e.getMessage());
      }
      throw new WriteProcessException(e);
    }

    long startTime = System.nanoTime();
    WALFlushListener walFlushListener;
    try {
      walFlushListener = walNode.log(workMemTable.getMemTableId(), insertTabletNode, start, end);
      if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
        throw walFlushListener.getCause();
      }
    } catch (Exception e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      rollbackMemoryInfo(memIncrements);
      throw new WriteProcessException(e);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(System.nanoTime() - startTime);
    }

    startTime = System.nanoTime();

    PipeDataNodeAgent.runtime().assignSimpleProgressIndexIfNeeded(insertTabletNode);
    if (!insertTabletNode.isGeneratedByPipe()) {
      workMemTable.markAsNotGeneratedByPipe();
    }
    PipeInsertionDataNodeListener.getInstance()
        .listenToInsertNode(
            dataRegionInfo.getDataRegion().getDataRegionId(),
            walFlushListener.getWalEntryHandler(),
            insertTabletNode,
            tsFileResource);

    try {
      if (insertTabletNode.isAligned()) {
        workMemTable.insertAlignedTablet(insertTabletNode, start, end);
      } else {
        workMemTable.insertTablet(insertTabletNode, start, end);
      }
    } catch (WriteProcessException e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      throw new WriteProcessException(e);
    }
    for (int i = start; i < end; i++) {
      results[i] = RpcUtils.SUCCESS_STATUS;
    }

    tsFileResource.updateStartTime(
        insertTabletNode.getDeviceID(), insertTabletNode.getTimes()[start]);
    // For sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // For unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertTabletNode.getDeviceID(), insertTabletNode.getTimes()[end - 1]);
    }

    tsFileResource.updateProgressIndex(insertTabletNode.getProgressIndex());

    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(System.nanoTime() - startTime);
  }

  @SuppressWarnings("squid:S3776") // High Cognitive Complexity
  private long[] checkMemCostAndAddToTspInfoForRow(
      IDeviceID deviceId, String[] measurements, TSDataType[] dataTypes, Object[] values)
      throws WriteProcessException {
    // Memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;

    for (int i = 0; i < dataTypes.length; i++) {
      // Skip failed Measurements
      if (dataTypes[i] == null || measurements[i] == null) {
        continue;
      }
      if (workMemTable.checkIfChunkDoesNotExist(deviceId, measurements[i])) {
        // ChunkMetadataIncrement
        chunkMetadataIncrement += ChunkMetadata.calculateRamSize(measurements[i], dataTypes[i]);
        memTableIncrement += TVList.tvListArrayMemCost(dataTypes[i]);
      } else {
        // here currentChunkPointNum >= 1
        long currentChunkPointNum = workMemTable.getCurrentTVListSize(deviceId, measurements[i]);
        memTableIncrement +=
            (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE) == 0
                ? TVList.tvListArrayMemCost(dataTypes[i])
                : 0;
      }
      // TEXT data mem size
      if (dataTypes[i].isBinary() && values[i] != null) {
        textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  @SuppressWarnings("squid:S3776") // High Cognitive Complexity
  private long[] checkMemCostAndAddToTspInfoForRows(InsertRowsNode insertRowsNode)
      throws WriteProcessException {
    // Memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;
    // device -> measurement -> adding TVList size
    Map<IDeviceID, Map<String, Integer>> increasingMemTableInfo = new HashMap<>();
    for (InsertRowNode insertRowNode : insertRowsNode.getInsertRowNodeList()) {
      IDeviceID deviceId = insertRowNode.getDeviceID();
      TSDataType[] dataTypes = insertRowNode.getDataTypes();
      Object[] values = insertRowNode.getValues();
      String[] measurements = insertRowNode.getMeasurements();
      for (int i = 0; i < dataTypes.length; i++) {
        // Skip failed Measurements
        if (dataTypes[i] == null || measurements[i] == null) {
          continue;
        }
        if (workMemTable.checkIfChunkDoesNotExist(deviceId, measurements[i])
            && (!increasingMemTableInfo.containsKey(deviceId)
                || !increasingMemTableInfo.get(deviceId).containsKey(measurements[i]))) {
          // ChunkMetadataIncrement
          chunkMetadataIncrement += ChunkMetadata.calculateRamSize(measurements[i], dataTypes[i]);
          memTableIncrement += TVList.tvListArrayMemCost(dataTypes[i]);
          increasingMemTableInfo
              .computeIfAbsent(deviceId, k -> new HashMap<>())
              .putIfAbsent(measurements[i], 1);
        } else {
          // here currentChunkPointNum >= 1
          long currentChunkPointNum = workMemTable.getCurrentTVListSize(deviceId, measurements[i]);
          int addingPointNum =
              increasingMemTableInfo
                  .computeIfAbsent(deviceId, k -> new HashMap<>())
                  .computeIfAbsent(measurements[i], k -> 0);
          memTableIncrement +=
              ((currentChunkPointNum + addingPointNum) % PrimitiveArrayManager.ARRAY_SIZE) == 0
                  ? TVList.tvListArrayMemCost(dataTypes[i])
                  : 0;
          increasingMemTableInfo.get(deviceId).computeIfPresent(measurements[i], (k, v) -> v + 1);
        }
        // TEXT data mem size
        if (dataTypes[i].isBinary() && values[i] != null) {
          textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
        }
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private long[] checkAlignedMemCostAndAddToTspInfoForRow(
      IDeviceID deviceId, String[] measurements, TSDataType[] dataTypes, Object[] values)
      throws WriteProcessException {
    // Memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;

    if (workMemTable.checkIfChunkDoesNotExist(deviceId, AlignedPath.VECTOR_PLACEHOLDER)) {
      // For new device of this mem table
      // ChunkMetadataIncrement
      chunkMetadataIncrement +=
          ChunkMetadata.calculateRamSize(AlignedPath.VECTOR_PLACEHOLDER, TSDataType.VECTOR)
              * dataTypes.length;
      memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypes);
      for (int i = 0; i < dataTypes.length; i++) {
        // Skip failed Measurements
        if (dataTypes[i] == null || measurements[i] == null) {
          continue;
        }
        // TEXT data mem size
        if (dataTypes[i].isBinary() && values[i] != null) {
          textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
        }
      }
    } else {
      // For existed device of this mem table
      AlignedWritableMemChunk alignedMemChunk =
          ((AlignedWritableMemChunkGroup) workMemTable.getMemTableMap().get(deviceId))
              .getAlignedMemChunk();
      List<TSDataType> dataTypesInTVList = new ArrayList<>();
      for (int i = 0; i < dataTypes.length; i++) {
        // Skip failed Measurements
        if (dataTypes[i] == null || measurements[i] == null) {
          continue;
        }

        // Extending the column of aligned mem chunk
        if (!alignedMemChunk.containsMeasurement(measurements[i])) {
          memTableIncrement +=
              (alignedMemChunk.alignedListSize() / PrimitiveArrayManager.ARRAY_SIZE + 1)
                  * AlignedTVList.valueListArrayMemCost(dataTypes[i]);
          dataTypesInTVList.add(dataTypes[i]);
        }
        // TEXT data mem size
        if (dataTypes[i].isBinary() && values[i] != null) {
          textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
        }
      }
      // Here currentChunkPointNum >= 1
      if ((alignedMemChunk.alignedListSize() % PrimitiveArrayManager.ARRAY_SIZE) == 0) {
        dataTypesInTVList.addAll(((AlignedTVList) alignedMemChunk.getTVList()).getTsDataTypes());
        memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypesInTVList);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private long[] checkAlignedMemCostAndAddToTspInfoForRows(InsertRowsNode insertRowsNode)
      throws WriteProcessException {
    // Memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;
    // device -> (measurements -> datatype, adding aligned TVList size)
    Map<IDeviceID, Pair<Map<String, TSDataType>, Integer>> increasingMemTableInfo = new HashMap<>();
    for (InsertRowNode insertRowNode : insertRowsNode.getInsertRowNodeList()) {
      IDeviceID deviceId = insertRowNode.getDeviceID();
      TSDataType[] dataTypes = insertRowNode.getDataTypes();
      Object[] values = insertRowNode.getValues();
      String[] measurements = insertRowNode.getMeasurements();
      if (workMemTable.checkIfChunkDoesNotExist(deviceId, AlignedPath.VECTOR_PLACEHOLDER)
          && !increasingMemTableInfo.containsKey(deviceId)) {
        // For new device of this mem table
        // ChunkMetadataIncrement
        chunkMetadataIncrement +=
            ChunkMetadata.calculateRamSize(AlignedPath.VECTOR_PLACEHOLDER, TSDataType.VECTOR)
                * dataTypes.length;
        memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypes);
        for (int i = 0; i < dataTypes.length; i++) {
          // Skip failed Measurements
          if (dataTypes[i] == null || measurements[i] == null) {
            continue;
          }
          increasingMemTableInfo
              .computeIfAbsent(deviceId, k -> new Pair<>(new HashMap<>(), 1))
              .left
              .put(measurements[i], dataTypes[i]);
          // TEXT data mem size
          if (dataTypes[i].isBinary() && values[i] != null) {
            textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
          }
        }

      } else {
        // For existed device of this mem table
        AlignedWritableMemChunkGroup memChunkGroup =
            (AlignedWritableMemChunkGroup) workMemTable.getMemTableMap().get(deviceId);
        AlignedWritableMemChunk alignedMemChunk =
            memChunkGroup == null ? null : memChunkGroup.getAlignedMemChunk();
        long currentChunkPointNum = alignedMemChunk == null ? 0 : alignedMemChunk.alignedListSize();
        List<TSDataType> dataTypesInTVList = new ArrayList<>();
        Pair<Map<String, TSDataType>, Integer> addingPointNumInfo =
            increasingMemTableInfo.computeIfAbsent(deviceId, k -> new Pair<>(new HashMap<>(), 0));
        for (int i = 0; i < dataTypes.length; i++) {
          // Skip failed Measurements
          if (dataTypes[i] == null || measurements[i] == null) {
            continue;
          }

          int addingPointNum = addingPointNumInfo.getRight();
          // Extending the column of aligned mem chunk
          if ((alignedMemChunk != null && !alignedMemChunk.containsMeasurement(measurements[i]))
              && !increasingMemTableInfo.get(deviceId).left.containsKey(measurements[i])) {
            memTableIncrement +=
                ((currentChunkPointNum + addingPointNum) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                    * AlignedTVList.valueListArrayMemCost(dataTypes[i]);
            increasingMemTableInfo.get(deviceId).left.put(measurements[i], dataTypes[i]);
          }
          // TEXT data mem size
          if (dataTypes[i].isBinary() && values[i] != null) {
            textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
          }
        }
        int addingPointNum = increasingMemTableInfo.get(deviceId).getRight();
        // Here currentChunkPointNum + addingPointNum >= 1
        if (((currentChunkPointNum + addingPointNum) % PrimitiveArrayManager.ARRAY_SIZE) == 0) {
          if (alignedMemChunk != null) {
            dataTypesInTVList.addAll(
                ((AlignedTVList) alignedMemChunk.getTVList()).getTsDataTypes());
          }
          dataTypesInTVList.addAll(increasingMemTableInfo.get(deviceId).getLeft().values());
          memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypesInTVList);
        }
        increasingMemTableInfo.get(deviceId).setRight(addingPointNum + 1);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  private long[] checkMemCostAndAddToTspInfoForTablet(
      IDeviceID deviceId,
      String[] measurements,
      TSDataType[] dataTypes,
      Object[] columns,
      int start,
      int end)
      throws WriteProcessException {
    if (start >= end) {
      return new long[] {0, 0, 0};
    }
    long[] memIncrements = new long[3]; // memTable, text, chunk metadata

    for (int i = 0; i < dataTypes.length; i++) {
      // Skip failed Measurements
      if (dataTypes[i] == null || columns[i] == null || measurements[i] == null) {
        continue;
      }
      updateMemCost(dataTypes[i], measurements[i], deviceId, start, end, memIncrements, columns[i]);
    }
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return memIncrements;
  }

  private long[] checkAlignedMemCostAndAddToTspForTablet(
      IDeviceID deviceId,
      String[] measurements,
      TSDataType[] dataTypes,
      Object[] columns,
      int start,
      int end)
      throws WriteProcessException {
    if (start >= end) {
      return new long[] {0, 0, 0};
    }
    long[] memIncrements = new long[3]; // memTable, text, chunk metadata

    updateAlignedMemCost(dataTypes, deviceId, measurements, start, end, memIncrements, columns);
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return memIncrements;
  }

  private void updateMemCost(
      TSDataType dataType,
      String measurement,
      IDeviceID deviceId,
      int start,
      int end,
      long[] memIncrements,
      Object column) {
    // memIncrements = [memTable, text, chunk metadata] respectively

    if (workMemTable.checkIfChunkDoesNotExist(deviceId, measurement)) {
      // ChunkMetadataIncrement
      memIncrements[2] += ChunkMetadata.calculateRamSize(measurement, dataType);
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * TVList.tvListArrayMemCost(dataType);
    } else {
      long currentChunkPointNum = workMemTable.getCurrentTVListSize(deviceId, measurement);
      if (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        memIncrements[0] +=
            ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                * TVList.tvListArrayMemCost(dataType);
      } else {
        long acquireArray =
            (end - start - 1 + (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
        if (acquireArray != 0) {
          memIncrements[0] += acquireArray * TVList.tvListArrayMemCost(dataType);
        }
      }
    }
    // TEXT data size
    if (dataType.isBinary()) {
      Binary[] binColumn = (Binary[]) column;
      memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
    }
  }

  private void updateAlignedMemCost(
      TSDataType[] dataTypes,
      IDeviceID deviceId,
      String[] measurementIds,
      int start,
      int end,
      long[] memIncrements,
      Object[] columns) {
    // memIncrements = [memTable, text, chunk metadata] respectively
    if (workMemTable.checkIfChunkDoesNotExist(deviceId, AlignedPath.VECTOR_PLACEHOLDER)) {
      // ChunkMetadataIncrement
      memIncrements[2] +=
          dataTypes.length
              * ChunkMetadata.calculateRamSize(AlignedPath.VECTOR_PLACEHOLDER, TSDataType.VECTOR);
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * AlignedTVList.alignedTvListArrayMemCost(dataTypes);
      for (int i = 0; i < dataTypes.length; i++) {
        TSDataType dataType = dataTypes[i];
        String measurement = measurementIds[i];
        Object column = columns[i];
        if (dataType == null || column == null || measurement == null) {
          continue;
        }
        // TEXT data size
        if (dataType.isBinary()) {
          Binary[] binColumn = (Binary[]) columns[i];
          memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
        }
      }

    } else {
      AlignedWritableMemChunk alignedMemChunk =
          ((AlignedWritableMemChunkGroup) workMemTable.getMemTableMap().get(deviceId))
              .getAlignedMemChunk();
      List<TSDataType> dataTypesInTVList = new ArrayList<>();
      for (int i = 0; i < dataTypes.length; i++) {
        TSDataType dataType = dataTypes[i];
        String measurement = measurementIds[i];
        Object column = columns[i];
        if (dataType == null || column == null || measurement == null) {
          continue;
        }
        // Extending the column of aligned mem chunk
        if (!alignedMemChunk.containsMeasurement(measurementIds[i])) {
          memIncrements[0] +=
              (alignedMemChunk.alignedListSize() / PrimitiveArrayManager.ARRAY_SIZE + 1)
                  * AlignedTVList.valueListArrayMemCost(dataType);
          dataTypesInTVList.add(dataType);
        }
        // TEXT data size
        if (dataType.isBinary()) {
          Binary[] binColumn = (Binary[]) columns[i];
          memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
        }
      }
      long acquireArray;
      if (alignedMemChunk.alignedListSize() % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        acquireArray = (end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1L;
      } else {
        acquireArray =
            (end
                    - start
                    - 1
                    + (alignedMemChunk.alignedListSize() % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
      }
      if (acquireArray != 0) {
        dataTypesInTVList.addAll(((AlignedTVList) alignedMemChunk.getTVList()).getTsDataTypes());
        memIncrements[0] +=
            acquireArray * AlignedTVList.alignedTvListArrayMemCost(dataTypesInTVList);
      }
    }
  }

  private void updateMemoryInfo(
      long memTableIncrement, long chunkMetadataIncrement, long textDataIncrement)
      throws WriteProcessRejectException {
    memTableIncrement += textDataIncrement;
    dataRegionInfo.addStorageGroupMemCost(memTableIncrement);
    tsFileProcessorInfo.addTSPMemCost(chunkMetadataIncrement);
    if (dataRegionInfo.needToReportToSystem()) {
      try {
        if (!SystemInfo.getInstance().reportStorageGroupStatus(dataRegionInfo, this)) {
          long startTime = System.currentTimeMillis();
          while (SystemInfo.getInstance().isRejected()) {
            if (workMemTable.shouldFlush()) {
              break;
            }
            try {
              TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
              if (System.currentTimeMillis() - startTime
                  > config.getMaxWaitingTimeWhenInsertBlocked()) {
                throw new WriteProcessRejectException(
                    "System rejected over " + (System.currentTimeMillis() - startTime) + "ms");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }
      } catch (WriteProcessRejectException e) {
        dataRegionInfo.releaseStorageGroupMemCost(memTableIncrement);
        tsFileProcessorInfo.releaseTSPMemCost(chunkMetadataIncrement);
        SystemInfo.getInstance().resetStorageGroupStatus(dataRegionInfo);
        throw e;
      }
    }
    workMemTable.addTVListRamCost(memTableIncrement);
    workMemTable.addTextDataSize(textDataIncrement);
  }

  private void rollbackMemoryInfo(long[] memIncrements) {
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];

    memTableIncrement += textDataIncrement;
    dataRegionInfo.releaseStorageGroupMemCost(memTableIncrement);
    tsFileProcessorInfo.releaseTSPMemCost(chunkMetadataIncrement);
    SystemInfo.getInstance().resetStorageGroupStatus(dataRegionInfo);
    workMemTable.releaseTVListRamCost(memTableIncrement);
    workMemTable.releaseTextDataSize(textDataIncrement);
  }

  /**
   * Delete data which belongs to the timeseries `deviceId.measurementId` and the timestamp of which
   * <= 'timestamp' in the deletion. <br>
   *
   * <p>Delete data in both working MemTable and flushing MemTables.
   */
  public void deleteDataInMemory(Deletion deletion, Set<PartialPath> devicePaths) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable != null) {
        logger.info(
            "[Deletion] Deletion with path: {}, time:{}-{} in workMemTable",
            deletion.getPath(),
            deletion.getStartTime(),
            deletion.getEndTime());
        for (PartialPath device : devicePaths) {
          workMemTable.delete(
              deletion.getPath(), device, deletion.getStartTime(), deletion.getEndTime());
        }
      }
      // Flushing memTables are immutable, only record this deletion in these memTables for read
      if (!flushingMemTables.isEmpty()) {
        modsToMemtable.add(new Pair<>(deletion, flushingMemTables.getLast()));
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  public WALFlushListener logDeleteDataNodeInWAL(DeleteDataNode deleteDataNode) {
    return walNode.log(workMemTable.getMemTableId(), deleteDataNode);
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  public boolean shouldFlush() {
    if (workMemTable == null) {
      return false;
    }
    if (workMemTable.shouldFlush()) {
      WritingMetrics.getInstance().recordMemControlFlushMemTableCount(1);
      return true;
    }
    if (workMemTable.reachChunkSizeOrPointNumThreshold()) {
      WritingMetrics.getInstance().recordSeriesFullFlushMemTableCount(1);
      return true;
    }
    return false;
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getTsFileSize();
    long fileSizeThreshold = sequence ? config.getSeqTsFileSize() : config.getUnSeqTsFileSize();
    return fileSize >= fileSizeThreshold;
  }

  public void syncClose() {
    logger.info(
        "Sync close file: {}, will firstly async close it",
        tsFileResource.getTsFile().getAbsolutePath());
    if (shouldClose) {
      return;
    }
    synchronized (flushingMemTables) {
      try {
        asyncClose();
        logger.info("Start to wait until file {} is closed", tsFileResource);
        long startTime = System.currentTimeMillis();
        while (!flushingMemTables.isEmpty()) {
          flushingMemTables.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000 && !flushingMemTables.isEmpty()) {
            logger.warn(
                "{} has spent {}s for waiting flushing one memtable; {} left (first: {}). FlushingManager info: {}",
                this.tsFileResource.getTsFile().getAbsolutePath(),
                (System.currentTimeMillis() - startTime) / 1000,
                flushingMemTables.size(),
                flushingMemTables.getFirst(),
                FlushManager.getInstance());
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait close interrupted",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getTsFile().getAbsolutePath());
  }

  /** async close one tsfile, register and close it by another thread */
  public Future<?> asyncClose() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (logger.isDebugEnabled()) {
        if (workMemTable != null) {
          logger.debug(
              "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile "
                  + "size: {}, plan index: [{}, {}], progress index: {}",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              workMemTable.memSize(),
              tsFileResource.getTsFileSize(),
              workMemTable.getMinPlanIndex(),
              workMemTable.getMaxPlanIndex(),
              tsFileResource.getMaxProgressIndex());
        } else {
          logger.debug(
              "{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              tsFileResource.getTsFileSize());
        }
      }

      if (shouldClose) {
        return CompletableFuture.completedFuture(null);
      }
      // when a flush thread serves this TsFileProcessor (because the processor is submitted by
      // registerTsFileProcessor()), the thread will seal the corresponding TsFile and
      // execute other cleanup works if "shouldClose == true and flushingMemTables is empty".

      // To ensure there must be a flush thread serving this processor after the field `shouldClose`
      // is set true, we need to generate a NotifyFlushMemTable as a signal task and submit it to
      // the FlushManager.

      // we have to add the memtable into flushingList first and then set the shouldClose tag.
      // see https://issues.apache.org/jira/browse/IOTDB-510
      IMemTable tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;

      try {
        PipeInsertionDataNodeListener.getInstance()
            .listenToTsFile(
                dataRegionInfo.getDataRegion().getDataRegionId(),
                tsFileResource,
                false,
                tmpMemTable.isTotallyGeneratedByPipe());

        // When invoke closing TsFile after insert data to memTable, we shouldn't flush until invoke
        // flushing memTable in System module.
        Future<?> future = addAMemtableIntoFlushingList(tmpMemTable);
        shouldClose = true;
        return future;
      } catch (Exception e) {
        logger.error(
            "{}: {} async close failed, because",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup Tips: I am
   * trying to solve this issue by checking whether the table exist before wait()
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled() && tmpMemTable.isSignalMemTable()) {
        logger.debug(
            "{}: {} add a signal memtable into flushing memtable list when sync flush",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }

    synchronized (flushingMemTables) {
      try {
        long startWait = System.currentTimeMillis();
        while (flushingMemTables.contains(tmpMemTable)) {
          flushingMemTables.wait(1000);

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn(
                "has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getTsFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait flush finished meets error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Put the working memtable into flushing list and set the working memtable to null */
  public void asyncFlush() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable == null) {
        return;
      }
      logger.info(
          "Async flush a memtable to tsfile: {}", tsFileResource.getTsFile().getAbsolutePath());
      addAMemtableIntoFlushingList(workMemTable);
    } catch (Exception e) {
      logger.error(
          "{}: {} add a memtable into flushing list failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * This method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private Future<?> addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    final Map<IDeviceID, Long> lastTimeForEachDevice = tobeFlushed.getMaxTime();

    // If some devices have been removed in MemTable, the number of device in MemTable and
    // tsFileResource will not be the same. And the endTime of these devices in resource will be
    // Long.minValue.
    // In the case, we need to delete the removed devices in tsFileResource.
    if (lastTimeForEachDevice.size() != tsFileResource.getDevices().size()) {
      tsFileResource.deleteRemovedDeviceAndUpdateEndTime(lastTimeForEachDevice);
    } else {
      if (sequence) {
        tsFileResource.updateEndTime(lastTimeForEachDevice);
      }
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onMemTableFlushStarted(tobeFlushed);
    }

    lastWorkMemtableFlushTime = System.currentTimeMillis();
    updateLatestFlushTimeCallback.call(this, lastTimeForEachDevice, lastWorkMemtableFlushTime);

    SystemInfo.getInstance().addFlushingMemTableCost(tobeFlushed.getTVListsRamCost());
    flushingMemTables.addLast(tobeFlushed);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          tobeFlushed.isSignalMemTable(),
          flushingMemTables.size());
    }

    if (!(tobeFlushed.isSignalMemTable() || tobeFlushed.isEmpty())) {
      totalMemTableSize += tobeFlushed.memSize();
    }
    WritingMetrics.getInstance()
        .recordMemTableLiveDuration(System.currentTimeMillis() - getWorkMemTableCreatedTime());
    WritingMetrics.getInstance()
        .recordActiveMemTableCount(dataRegionInfo.getDataRegion().getDataRegionId(), -1);
    workMemTable = null;
    return FlushManager.getInstance().registerTsFileProcessor(this);
  }

  /** Put back the memtable to MemTablePool and make metadata in writer visible */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      writer.makeMetadataVisible();
      if (!flushingMemTables.remove(memTable)) {
        logger.warn(
            "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable());
      } else if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} memtable (signal={}) is removed from the queue. {} left.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable(),
            flushingMemTables.size());
      }
      memTable.release();
      MemTableManager.getInstance().decreaseMemtableNumber();
      // Reset the mem cost in StorageGroupProcessorInfo
      dataRegionInfo.releaseStorageGroupMemCost(memTable.getTVListsRamCost());
      if (logger.isDebugEnabled()) {
        logger.debug(
            "[mem control] {}: {} flush finished, try to reset system memcost, "
                + "flushing memtable list size: {}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            flushingMemTables.size());
      }
      // Report to System
      SystemInfo.getInstance().resetStorageGroupStatus(dataRegionInfo);
      SystemInfo.getInstance().resetFlushingMemTableCost(memTable.getTVListsRamCost());
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} flush finished, remove a memtable from flushing list, "
                + "flushing memtable list size: {}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            flushingMemTables.size());
      }
    } catch (Exception e) {
      logger.error("{}: {}", storageGroupName, tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /** This method will synchronize the memTable and release its flushing resources */
  private void syncReleaseFlushedMemTable(IMemTable memTable) {
    synchronized (flushingMemTables) {
      releaseFlushedMemTable(memTable);
      flushingMemTables.notifyAll();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} released a memtable (signal={}), flushingMemtables size ={}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable(),
            flushingMemTables.size());
      }
    }
  }

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  @SuppressWarnings({"squid:S3776", "squid:S2142"}) // Suppress high Cognitive Complexity warning
  public void flushOneMemTable() {
    IMemTable memTableToFlush = flushingMemTables.getFirst();

    // Signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      if (memTableToFlush.isEmpty()) {
        logger.info(
            "This normal memtable is empty, skip flush. {}: {}",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      } else {
        try {
          writer.mark();
          MemTableFlushTask flushTask =
              new MemTableFlushTask(
                  memTableToFlush,
                  writer,
                  storageGroupName,
                  dataRegionInfo.getDataRegion().getDataRegionId());
          flushTask.syncFlushMemTable();
          memTableFlushPointCount = memTableToFlush.getTotalPointsNum();
        } catch (Throwable e) {
          if (writer == null) {
            logger.info(
                "{}: {} is closed during flush, abandon flush task",
                storageGroupName,
                tsFileResource.getTsFile().getAbsolutePath());
            synchronized (flushingMemTables) {
              flushingMemTables.notifyAll();
            }
          } else {
            logger.error(
                "{}: {} meet error when flushing a memtable, change system mode to error",
                storageGroupName,
                tsFileResource.getTsFile().getAbsolutePath(),
                e);
            CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
            try {
              logger.error(
                  "{}: {} IOTask meets error, truncate the corrupted data",
                  storageGroupName,
                  tsFileResource.getTsFile().getAbsolutePath(),
                  e);
              writer.reset();
            } catch (IOException e1) {
              logger.error(
                  "{}: {} Truncate corrupted data meets error",
                  storageGroupName,
                  tsFileResource.getTsFile().getAbsolutePath(),
                  e1);
            }
            // Release resource
            try {
              syncReleaseFlushedMemTable(memTableToFlush);
              // Make sure no read will search this file
              tsFileResource.setTimeIndex(config.getTimeIndexLevel().getTimeIndex());
              // This callback method will register this empty tsfile into TsFileManager
              for (CloseFileListener closeFileListener : closeFileListeners) {
                closeFileListener.onClosed(this);
              }
              // Close writer
              writer.close();
              writer = null;
              synchronized (flushingMemTables) {
                flushingMemTables.notifyAll();
              }
            } catch (Exception e1) {
              logger.error(
                  "{}: {} Release resource meets error",
                  storageGroupName,
                  tsFileResource.getTsFile().getAbsolutePath(),
                  e1);
            }
            return;
          }
        }
      }
    }

    try {
      flushQueryLock.writeLock().lock();
      Iterator<Pair<Modification, IMemTable>> iterator = modsToMemtable.iterator();
      while (iterator.hasNext()) {
        Pair<Modification, IMemTable> entry = iterator.next();
        if (entry.right.equals(memTableToFlush)) {
          entry.left.setFileOffset(tsFileResource.getTsFileSize());
          this.tsFileResource.getModFile().write(entry.left);
          tsFileResource.getModFile().close();
          iterator.remove();
          logger.info(
              "[Deletion] Deletion with path: {}, time:{}-{} written when flush memtable",
              entry.left.getPath(),
              ((Deletion) (entry.left)).getStartTime(),
              ((Deletion) (entry.left)).getEndTime());
        }
      }
    } catch (IOException e) {
      logger.error(
          "Meet error when writing into ModificationFile file of {} ",
          tsFileResource.getTsFile().getAbsolutePath(),
          e);
    } finally {
      flushQueryLock.writeLock().unlock();
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} try get lock to release a memtable (signal={})",
          storageGroupName,
          tsFileResource.getTsFile().getAbsolutePath(),
          memTableToFlush.isSignalMemTable());
    }

    // For sync flush
    syncReleaseFlushedMemTable(memTableToFlush);
    try {
      writer.getTsFileOutput().force();
    } catch (IOException e) {
      logger.error("fsync memTable data to disk error,", e);
    }

    // Call flushed listener after memtable is released safely
    for (FlushListener flushListener : flushListeners) {
      flushListener.onMemTableFlushed(memTableToFlush);
    }
    // Retry to avoid unnecessary read-only mode
    int retryCnt = 0;
    while (shouldClose && flushingMemTables.isEmpty() && writer != null) {
      try {
        if (isEmpty()) {
          endEmptyFile();
        } else {
          writer.mark();
          updateCompressionRatio();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: {} flushingMemtables is empty and will close the file",
                storageGroupName,
                tsFileResource.getTsFile().getAbsolutePath());
          }
          endFile();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("{} flushingMemtables is clear", storageGroupName);
        }
      } catch (Exception e) {
        logger.error(
            "{}: {} marking or ending file meet error",
            storageGroupName,
            tsFileResource.getTsFile().getAbsolutePath(),
            e);
        // Truncate broken metadata
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error(
              "{}: {} truncate corrupted data meets error",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              e1);
        }
        // Retry or set read-only
        if (retryCnt < 3) {
          logger.warn(
              "{} meet error when flush FileMetadata to {}, retry it again",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              e);
          retryCnt++;
          continue;
        } else {
          logger.error(
              "{} meet error when flush FileMetadata to {}, change system mode to error",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              e);
          CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
          break;
        }
      }
      // For sync close
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} try to get flushingMemtables lock.",
            storageGroupName,
            tsFileResource.getTsFile().getAbsolutePath());
      }
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    }
  }

  private void updateCompressionRatio() {
    try {
      double compressionRatio = ((double) totalMemTableSize) / writer.getPos();
      logger.info(
          "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}",
          writer.getFile().getAbsolutePath(),
          String.format("%.2f", compressionRatio),
          totalMemTableSize,
          writer.getPos());
      String dataRegionId = dataRegionInfo.getDataRegion().getDataRegionId();
      WritingMetrics.getInstance()
          .recordTsFileCompressionRatioOfFlushingMemTable(dataRegionId, compressionRatio);
      CompressionRatio.getInstance().updateRatio(totalMemTableSize, writer.getPos());
    } catch (IOException e) {
      logger.error(
          "{}: {} update compression ratio failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    }
  }

  /** end file and write some meta */
  private void endFile() throws IOException, TsFileProcessorException {
    if (logger.isDebugEnabled()) {
      logger.debug("Start to end file {}", tsFileResource);
    }
    writer.endFile();
    tsFileResource.serialize();
    if (logger.isDebugEnabled()) {
      logger.debug("Ended file {}", tsFileResource);
    }
    // Remove this processor from Closing list in StorageGroupProcessor,
    // Mark the TsFileResource closed, no need writer anymore
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this);
    }

    tsFileProcessorInfo.clear();
    dataRegionInfo.closeTsFileProcessorAndReportToSystem(this);

    writer = null;
  }

  /** End empty file and remove it from file system */
  private void endEmptyFile() throws TsFileProcessorException, IOException {
    logger.info("Start to end empty file {}", tsFileResource);

    // Remove this processor from Closing list in DataRegion,
    // Mark the TsFileResource closed, no need writer anymore
    writer.close();
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this);
    }
    tsFileProcessorInfo.clear();
    dataRegionInfo.closeTsFileProcessorAndReportToSystem(this);
    logger.info(
        "Storage group {} close and remove empty file {}",
        storageGroupName,
        tsFileResource.getTsFile().getAbsoluteFile());

    writer = null;
  }

  public boolean isManagedByFlushManager() {
    return managedByFlushManager;
  }

  public void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  /** Close this tsfile */
  public void close() throws TsFileProcessorException {
    try {
      // When closing resource file, its corresponding mod file is also closed.
      tsFileResource.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  private void processAlignedChunkMetaDataFromFlushedMemTable(
      IDeviceID deviceID,
      AlignedChunkMetadata alignedChunkMetadata,
      Map<String, List<IChunkMetadata>> measurementToChunkMetaMap,
      Map<String, List<IChunkHandle>> measurementToChunkHandleMap,
      String filePath) {
    SharedTimeDataBuffer sharedTimeDataBuffer =
        new SharedTimeDataBuffer(alignedChunkMetadata.getTimeChunkMetadata());
    for (IChunkMetadata valueChunkMetaData : alignedChunkMetadata.getValueChunkMetadataList()) {
      String measurement = valueChunkMetaData.getMeasurementUid();
      measurementToChunkMetaMap
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(valueChunkMetaData);
      measurementToChunkHandleMap
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(
              new DiskAlignedChunkHandleImpl(
                  deviceID,
                  measurement,
                  filePath,
                  false,
                  valueChunkMetaData.getOffsetOfChunkHeader(),
                  valueChunkMetaData.getStatistics(),
                  sharedTimeDataBuffer));
    }
  }

  private void processChunkMetaDataFromFlushedMemTable(
      IDeviceID deviceID,
      ChunkMetadata chunkMetadata,
      Map<String, List<IChunkMetadata>> measurementToChunkMetaMap,
      Map<String, List<IChunkHandle>> measurementToChunkHandleMap,
      String filePath) {
    String measurement = chunkMetadata.getMeasurementUid();
    measurementToChunkMetaMap
        .computeIfAbsent(measurement, k -> new ArrayList<>())
        .add(chunkMetadata);
    measurementToChunkHandleMap
        .computeIfAbsent(measurement, k -> new ArrayList<>())
        .add(
            new DiskChunkHandleImpl(
                deviceID,
                measurement,
                filePath,
                false,
                chunkMetadata.getOffsetOfChunkHeader(),
                chunkMetadata.getStatistics()));
  }

  private void buildChunkHandleForFlushedMemTable(
      IDeviceID deviceID,
      List<IChunkMetadata> chunkMetadataList,
      Map<String, List<IChunkMetadata>> measurementToChunkMetaList,
      Map<String, List<IChunkHandle>> measurementToChunkHandleList) {
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (chunkMetadata instanceof AlignedChunkMetadata) {
        processAlignedChunkMetaDataFromFlushedMemTable(
            deviceID,
            (AlignedChunkMetadata) chunkMetadata,
            measurementToChunkMetaList,
            measurementToChunkHandleList,
            this.tsFileResource.getTsFilePath());
      } else {
        processChunkMetaDataFromFlushedMemTable(
            deviceID,
            (ChunkMetadata) chunkMetadata,
            measurementToChunkMetaList,
            measurementToChunkHandleList,
            this.tsFileResource.getTsFilePath());
      }
    }
  }

  private int searchTimeChunkMetaDataIndexAndSetModifications(
      List<List<ChunkMetadata>> chunkMetaDataList,
      IDeviceID deviceID,
      List<List<Modification>> modifications,
      QueryContext context)
      throws QueryProcessException {
    int timeChunkMetaDataIndex = -1;
    for (int i = 0; i < chunkMetaDataList.size(); i++) {
      List<ChunkMetadata> chunkMetadata = chunkMetaDataList.get(i);
      String measurement = chunkMetadata.get(0).getMeasurementUid();
      // measurement = "" means this is a timeChunkMetadata
      if (measurement.isEmpty()) {
        timeChunkMetaDataIndex = i;
        continue;
      }

      try {
        modifications.add(context.getPathModifications(tsFileResource, deviceID, measurement));
      } catch (IllegalPathException e) {
        throw new QueryProcessException(e.getMessage());
      }
    }
    return timeChunkMetaDataIndex;
  }

  private List<IChunkMetadata> getVisibleMetadataListFromWriterByDeviceID(
      QueryContext queryContext, IDeviceID deviceID) throws IllegalPathException {
    long timeLowerBound =
        getQueryTimeLowerBound(
            PathUtils.splitPathToDetachedNodes(((PlainDeviceID) deviceID).toStringID()));
    List<List<ChunkMetadata>> chunkMetaDataListForDevice =
        writer.getVisibleMetadataList(deviceID, null);
    List<ChunkMetadata> processedChunkMetadataForOneDevice = new ArrayList<>();
    for (List<ChunkMetadata> chunkMetadataList : chunkMetaDataListForDevice) {
      if (chunkMetadataList.isEmpty()) {
        continue;
      }
      ModificationUtils.modifyChunkMetaData(
          chunkMetadataList,
          queryContext.getPathModifications(
              tsFileResource, deviceID, chunkMetadataList.get(0).getMeasurementUid()));
      chunkMetadataList.removeIf(x -> x.getEndTime() < timeLowerBound);
      processedChunkMetadataForOneDevice.addAll(chunkMetadataList);
    }
    return new ArrayList<>(processedChunkMetadataForOneDevice);
  }

  private List<IChunkMetadata> getAlignedVisibleMetadataListFromWriterByDeviceID(
      QueryContext queryContext, IDeviceID deviceID)
      throws QueryProcessException, IllegalPathException {
    List<AlignedChunkMetadata> alignedChunkMetadataForOneDevice = new ArrayList<>();
    List<List<Modification>> modifications = new ArrayList<>();
    List<List<ChunkMetadata>> chunkMetaDataListForDevice =
        writer.getVisibleMetadataList(deviceID, null);

    if (chunkMetaDataListForDevice.isEmpty()) {
      return Collections.emptyList();
    }

    int timeChunkMetadataListIndex =
        searchTimeChunkMetaDataIndexAndSetModifications(
            chunkMetaDataListForDevice, deviceID, modifications, queryContext);
    if (timeChunkMetadataListIndex == -1) {
      throw new QueryProcessException("TimeChunkMetadata in aligned device should not be empty");
    }
    List<ChunkMetadata> timeChunkMetadataList =
        chunkMetaDataListForDevice.get(timeChunkMetadataListIndex);

    for (int i = 0; i < timeChunkMetadataList.size(); i++) {
      List<IChunkMetadata> valuesChunkMetadata = new ArrayList<>();
      boolean exits = false;
      for (int j = 0; j < chunkMetaDataListForDevice.size(); j++) {
        List<ChunkMetadata> chunkMetadataList = chunkMetaDataListForDevice.get(j);
        // Filter timeChunkMetadata
        if (j == timeChunkMetadataListIndex || chunkMetadataList.isEmpty()) {
          continue;
        }
        boolean currentExist = i < chunkMetadataList.size();
        exits = (exits || currentExist);
        valuesChunkMetadata.add(currentExist ? chunkMetadataList.get(i) : null);
      }
      if (exits) {
        alignedChunkMetadataForOneDevice.add(
            new AlignedChunkMetadata(timeChunkMetadataList.get(i), valuesChunkMetadata));
      }
    }

    long timeLowerBound =
        getQueryTimeLowerBound(
            PathUtils.splitPathToDetachedNodes(((PlainDeviceID) deviceID).toStringID()));
    ModificationUtils.modifyAlignedChunkMetaData(alignedChunkMetadataForOneDevice, modifications);
    alignedChunkMetadataForOneDevice.removeIf(x -> x.getEndTime() < timeLowerBound);
    return new ArrayList<>(alignedChunkMetadataForOneDevice);
  }

  public void queryForSeriesRegionScan(
      List<PartialPath> pathList,
      QueryContext queryContext,
      List<IFileScanHandle> fileScanHandlesForQuery) {
    long startTime = System.nanoTime();
    try {
      Map<IDeviceID, Map<String, List<IChunkHandle>>> deviceToMemChunkHandleMap = new HashMap<>();
      Map<IDeviceID, Map<String, List<IChunkMetadata>>> deviceToChunkMetadataListMap =
          new HashMap<>();
      flushQueryLock.readLock().lock();
      try {
        for (PartialPath seriesPath : pathList) {
          Map<String, List<IChunkMetadata>> measurementToChunkMetaList = new HashMap<>();
          Map<String, List<IChunkHandle>> measurementToChunkHandleList = new HashMap<>();
          long timeLowerBound = getQueryTimeLowerBound(seriesPath.getDevicePath().getNodes());
          for (IMemTable flushingMemTable : flushingMemTables) {
            if (flushingMemTable.isSignalMemTable()) {
              continue;
            }
            flushingMemTable.queryForSeriesRegionScan(
                seriesPath,
                timeLowerBound,
                measurementToChunkMetaList,
                measurementToChunkHandleList,
                modsToMemtable);
          }
          if (workMemTable != null) {
            workMemTable.queryForSeriesRegionScan(
                seriesPath,
                timeLowerBound,
                measurementToChunkMetaList,
                measurementToChunkHandleList,
                null);
          }
          IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(seriesPath.getDevice());
          // Some memTable have been flushed already, so we need to get the chunk metadata from
          // writer and build chunk handle for disk scanning
          buildChunkHandleForFlushedMemTable(
              deviceID,
              ResourceByPathUtils.getResourceInstance(seriesPath)
                  .getVisibleMetadataListFromWriter(
                      writer, tsFileResource, queryContext, timeLowerBound),
              measurementToChunkMetaList,
              measurementToChunkHandleList);

          if (!measurementToChunkHandleList.isEmpty() || !measurementToChunkMetaList.isEmpty()) {
            deviceToMemChunkHandleMap
                .computeIfAbsent(deviceID, k -> new HashMap<>())
                .putAll(measurementToChunkHandleList);
            deviceToChunkMetadataListMap
                .computeIfAbsent(deviceID, k -> new HashMap<>())
                .putAll(measurementToChunkMetaList);
          }
        }
      } catch (QueryProcessException | MetadataException | IOException e) {
        logger.error(
            "{}: {} get ReadOnlyMemChunk has error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      } finally {
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(FLUSHING_MEMTABLE, flushingMemTables.size());
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(
            WORKING_MEMTABLE, workMemTable != null ? 1 : 0);

        flushQueryLock.readLock().unlock();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: {} release flushQueryLock",
              storageGroupName,
              tsFileResource.getTsFile().getName());
        }
      }
      if (!deviceToMemChunkHandleMap.isEmpty() || !deviceToChunkMetadataListMap.isEmpty()) {
        fileScanHandlesForQuery.add(
            new UnclosedFileScanHandleImpl(
                deviceToChunkMetadataListMap, deviceToMemChunkHandleMap, tsFileResource));
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          GET_QUERY_RESOURCE_FROM_MEM, System.nanoTime() - startTime);
    }
  }

  /**
   * Construct IFileScanHandle for data in memtable and the other ones in flushing memtables. Then
   * get the related ChunkMetadata of data on disk.
   */
  public void queryForDeviceRegionScan(
      Map<IDeviceID, DeviceContext> devicePathsToContext,
      QueryContext queryContext,
      List<IFileScanHandle> fileScanHandlesForQuery) {
    long startTime = System.nanoTime();
    try {
      Map<IDeviceID, Map<String, List<IChunkHandle>>> deviceToMemChunkHandleMap = new HashMap<>();
      Map<IDeviceID, Map<String, List<IChunkMetadata>>> deviceToChunkMetadataListMap =
          new HashMap<>();
      flushQueryLock.readLock().lock();
      try {
        for (Map.Entry<IDeviceID, DeviceContext> entry : devicePathsToContext.entrySet()) {
          IDeviceID deviceID = entry.getKey();
          boolean isAligned = entry.getValue().isAligned();
          long timeLowerBound =
              getQueryTimeLowerBound(
                  PathUtils.splitPathToDetachedNodes(((PlainDeviceID) deviceID).toStringID()));
          Map<String, List<IChunkMetadata>> measurementToChunkMetadataList = new HashMap<>();
          Map<String, List<IChunkHandle>> measurementToMemChunkHandleList = new HashMap<>();
          for (IMemTable flushingMemTable : flushingMemTables) {
            if (flushingMemTable.isSignalMemTable()) {
              continue;
            }
            flushingMemTable.queryForDeviceRegionScan(
                deviceID,
                isAligned,
                timeLowerBound,
                measurementToChunkMetadataList,
                measurementToMemChunkHandleList,
                modsToMemtable);
          }
          if (workMemTable != null) {
            workMemTable.queryForDeviceRegionScan(
                deviceID,
                isAligned,
                timeLowerBound,
                measurementToChunkMetadataList,
                measurementToMemChunkHandleList,
                null);
          }

          buildChunkHandleForFlushedMemTable(
              deviceID,
              isAligned
                  ? getAlignedVisibleMetadataListFromWriterByDeviceID(queryContext, deviceID)
                  : getVisibleMetadataListFromWriterByDeviceID(queryContext, deviceID),
              measurementToChunkMetadataList,
              measurementToMemChunkHandleList);

          if (!measurementToMemChunkHandleList.isEmpty()
              || !measurementToChunkMetadataList.isEmpty()) {
            deviceToMemChunkHandleMap.put(deviceID, measurementToMemChunkHandleList);
            deviceToChunkMetadataListMap.put(deviceID, measurementToChunkMetadataList);
          }
        }
      } catch (QueryProcessException | MetadataException | IOException e) {
        logger.error(
            "{}: {} get ReadOnlyMemChunk has error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      } finally {
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(FLUSHING_MEMTABLE, flushingMemTables.size());
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(
            WORKING_MEMTABLE, workMemTable != null ? 1 : 0);

        flushQueryLock.readLock().unlock();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: {} release flushQueryLock",
              storageGroupName,
              tsFileResource.getTsFile().getName());
        }
      }

      if (!deviceToMemChunkHandleMap.isEmpty() || !deviceToChunkMetadataListMap.isEmpty()) {
        fileScanHandlesForQuery.add(
            new UnclosedFileScanHandleImpl(
                deviceToChunkMetadataListMap, deviceToMemChunkHandleMap, tsFileResource));
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          GET_QUERY_RESOURCE_FROM_MEM, System.nanoTime() - startTime);
    }
  }

  /**
   * Get the chunk(s) in the memtable (one from work memtable and the other ones in flushing
   * memtables and then compact them into one TimeValuePairSorter). Then get the related
   * ChunkMetadata of data on disk.
   *
   * @param seriesPaths selected paths
   */
  public void query(
      List<PartialPath> seriesPaths,
      QueryContext context,
      List<TsFileResource> tsfileResourcesForQuery)
      throws IOException {
    long startTime = System.nanoTime();
    try {
      Map<PartialPath, List<IChunkMetadata>> pathToChunkMetadataListMap = new HashMap<>();
      Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap = new HashMap<>();

      flushQueryLock.readLock().lock();
      try {
        for (PartialPath seriesPath : seriesPaths) {
          List<ReadOnlyMemChunk> readOnlyMemChunks = new ArrayList<>();
          long timeLowerBound = getQueryTimeLowerBound(seriesPath.getDevicePath().getNodes());
          for (IMemTable flushingMemTable : flushingMemTables) {
            if (flushingMemTable.isSignalMemTable()) {
              continue;
            }
            ReadOnlyMemChunk memChunk =
                flushingMemTable.query(context, seriesPath, timeLowerBound, modsToMemtable);
            if (memChunk != null) {
              readOnlyMemChunks.add(memChunk);
            }
          }
          if (workMemTable != null) {
            ReadOnlyMemChunk memChunk =
                workMemTable.query(context, seriesPath, timeLowerBound, null);
            if (memChunk != null) {
              readOnlyMemChunks.add(memChunk);
            }
          }

          List<IChunkMetadata> chunkMetadataList =
              ResourceByPathUtils.getResourceInstance(seriesPath)
                  .getVisibleMetadataListFromWriter(
                      writer, tsFileResource, context, timeLowerBound);

          // get in memory data
          if (!readOnlyMemChunks.isEmpty() || !chunkMetadataList.isEmpty()) {
            pathToReadOnlyMemChunkMap.put(seriesPath, readOnlyMemChunks);
            pathToChunkMetadataListMap.put(seriesPath, chunkMetadataList);
          }
        }
      } catch (QueryProcessException | MetadataException e) {
        logger.error(
            "{}: {} get ReadOnlyMemChunk has error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      } finally {
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(FLUSHING_MEMTABLE, flushingMemTables.size());
        QUERY_RESOURCE_METRICS.recordQueryResourceNum(
            WORKING_MEMTABLE, workMemTable != null ? 1 : 0);

        flushQueryLock.readLock().unlock();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: {} release flushQueryLock",
              storageGroupName,
              tsFileResource.getTsFile().getName());
        }
      }

      if (!pathToReadOnlyMemChunkMap.isEmpty() || !pathToChunkMetadataListMap.isEmpty()) {
        tsfileResourcesForQuery.add(
            new TsFileResource(
                pathToReadOnlyMemChunkMap, pathToChunkMetadataListMap, tsFileResource));
      }
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          GET_QUERY_RESOURCE_FROM_MEM, System.nanoTime() - startTime);
    }
  }

  private long getQueryTimeLowerBound(String[] device) {
    long deviceTTL = DataNodeTTLCache.getInstance().getTTL(device);
    return deviceTTL != Long.MAX_VALUE
        ? CommonDateTimeUtils.currentTime() - deviceTTL
        : Long.MIN_VALUE;
  }

  public long getTimeRangeId() {
    return timeRangeId;
  }

  public void setTimeRangeId(long timeRangeId) {
    this.timeRangeId = timeRangeId;
  }

  /** Release resource of a memtable */
  public void putMemTableBackAndClose() throws TsFileProcessorException {
    if (workMemTable != null) {
      workMemTable.release();
      dataRegionInfo.releaseStorageGroupMemCost(workMemTable.getTVListsRamCost());
      workMemTable = null;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    tsFileProcessorInfo.clear();
    dataRegionInfo.closeTsFileProcessorAndReportToSystem(this);
  }

  public void setTsFileProcessorInfo(TsFileProcessorInfo tsFileProcessorInfo) {
    this.tsFileProcessorInfo = tsFileProcessorInfo;
  }

  public long getWorkMemTableRamCost() {
    return workMemTable != null ? workMemTable.getTVListsRamCost() : 0;
  }

  /** Return Long.MAX_VALUE if workMemTable is null */
  public long getWorkMemTableCreatedTime() {
    return workMemTable != null ? workMemTable.getCreatedTime() : Long.MAX_VALUE;
  }

  /** Return Long.MAX_VALUE if workMemTable is null */
  public long getWorkMemTableUpdateTime() {
    return workMemTable != null ? workMemTable.getUpdateTime() : Long.MAX_VALUE;
  }

  public long getMemTableFlushPointCount() {
    return memTableFlushPointCount;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void setWorkMemTableShouldFlush() {
    workMemTable.setShouldFlush();
  }

  public void addCloseFileListener(CloseFileListener listener) {
    closeFileListeners.add(listener);
  }

  public void addFlushListeners(Collection<FlushListener> listeners) {
    flushListeners.addAll(listeners);
  }

  public void addCloseFileListeners(Collection<CloseFileListener> listeners) {
    closeFileListeners.addAll(listeners);
  }

  public void submitAFlushTask() {
    this.dataRegionInfo.getDataRegion().submitAFlushTaskWhenShouldFlush(this);
  }

  public boolean alreadyMarkedClosing() {
    return shouldClose;
  }

  public boolean isEmpty() {
    return totalMemTableSize == 0
        && (workMemTable == null || workMemTable.getTotalPointsNum() == 0);
  }

  @TestOnly
  public IMemTable getWorkMemTable() {
    return workMemTable;
  }

  @TestOnly
  public ConcurrentLinkedDeque<IMemTable> getFlushingMemTable() {
    return flushingMemTables;
  }
}
