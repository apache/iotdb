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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.ResourceByPathUtils;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;
import org.apache.iotdb.db.storageengine.rescon.memory.MemTableManager;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.GET_QUERY_RESOURCE_FROM_MEM;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.FLUSHING_MEMTABLE;
import static org.apache.iotdb.db.queryengine.metric.QueryResourceMetricSet.WORKING_MEMTABLE;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileProcessor {

  /** logger fot this class. */
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);

  /** storgae group name of this tsfile. */
  private final String storageGroupName;

  /** IoTDB config. */
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** whether it's enable mem control. */
  private final boolean enableMemControl = config.isEnableMemControl();

  /** database info for mem control. */
  private final DataRegionInfo dataRegionInfo;
  /** tsfile processor info for mem control. */
  private TsFileProcessorInfo tsFileProcessorInfo;

  /** sync this object in read() and asyncTryToFlush(). */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  /** modification to memtable mapping. */
  private final List<Pair<Modification, IMemTable>> modsToMemtable = new ArrayList<>();

  /** writer for restore tsfile and flushing. */
  private RestorableTsFileIOWriter writer;

  /** tsfile resource for index this tsfile. */
  private final TsFileResource tsFileResource;

  /** time range index to indicate this processor belongs to which time range */
  private long timeRangeId;
  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;

  /** a lock to mutual exclude read and read */
  private final ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();
  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;

  /** working memtable. */
  private IMemTable workMemTable;

  /** last flush time to flush the working memtable. */
  private long lastWorkMemtableFlushTime;

  /** this callback is called before the workMemtable is added into the flushingMemTables. */
  private final DataRegion.UpdateEndTimeCallBack updateLatestFlushTimeCallback;

  /** wal node. */
  private final IWALNode walNode;

  /** whether it's a sequence file or not. */
  private final boolean sequence;

  /** total memtable size for mem control. */
  private long totalMemTableSize;

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE =
      "{}: {} get flushQueryLock write lock released";

  /** close file listener. */
  private final List<CloseFileListener> closeFileListeners = new ArrayList<>();

  /** flush file listener. */
  private final List<FlushListener> flushListeners = new ArrayList<>();

  private final QueryExecutionMetricSet QUERY_EXECUTION_METRICS =
      QueryExecutionMetricSet.getInstance();
  private final QueryResourceMetricSet QUERY_RESOURCE_METRICS =
      QueryResourceMetricSet.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

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
   * insert data in an InsertRowNode into the workingMemtable.
   *
   * @param insertRowNode physical plan of insertion
   */
  public void insert(InsertRowNode insertRowNode) throws WriteProcessException {

    if (workMemTable == null) {
      long startTime = System.nanoTime();
      createNewWorkingMemTable();
      PERFORMANCE_OVERVIEW_METRICS.recordCreateMemtableBlockCost(System.nanoTime() - startTime);
    }

    long[] memIncrements = null;
    if (enableMemControl) {
      long startTime = System.nanoTime();
      if (insertRowNode.isAligned()) {
        memIncrements =
            checkAlignedMemCostAndAddToTspInfo(
                insertRowNode.getDevicePath().getFullPath(), insertRowNode.getMeasurements(),
                insertRowNode.getDataTypes(), insertRowNode.getValues());
      } else {
        memIncrements =
            checkMemCostAndAddToTspInfo(
                insertRowNode.getDevicePath().getFullPath(), insertRowNode.getMeasurements(),
                insertRowNode.getDataTypes(), insertRowNode.getValues());
      }
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(System.nanoTime() - startTime);
    }

    long startTime = System.nanoTime();
    WALFlushListener walFlushListener;
    try {
      walFlushListener = walNode.log(workMemTable.getMemTableId(), insertRowNode);
      if (walFlushListener.waitForResult() == WALFlushListener.Status.FAILURE) {
        throw walFlushListener.getCause();
      }
    } catch (Exception e) {
      if (enableMemControl) {
        rollbackMemoryInfo(memIncrements);
      }
      throw new WriteProcessException(
          String.format(
              "%s: %s write WAL failed",
              storageGroupName, tsFileResource.getTsFile().getAbsolutePath()),
          e);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(System.nanoTime() - startTime);
    }

    startTime = System.nanoTime();

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

    // update start time of this memtable
    tsFileResource.updateStartTime(
        insertRowNode.getDeviceID().toStringID(), insertRowNode.getTime());
    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertRowNode.getDeviceID().toStringID(), insertRowNode.getTime());
    }

    tsFileResource.updateProgressIndex(insertRowNode.getProgressIndex());

    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(System.nanoTime() - startTime);
  }

  private void createNewWorkingMemTable() throws WriteProcessException {
    workMemTable = MemTableManager.getInstance().getAvailableMemTable(storageGroupName);
    walNode.onMemTableCreated(workMemTable, tsFileResource.getTsFilePath());
  }

  /**
   * insert batch data of insertTabletPlan into the workingMemtable. The rows to be inserted are in
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
    }

    long[] memIncrements = null;
    try {
      if (enableMemControl) {
        long startTime = System.nanoTime();
        if (insertTabletNode.isAligned()) {
          memIncrements =
              checkAlignedMemCostAndAddToTsp(
                  insertTabletNode.getDevicePath().getFullPath(),
                  insertTabletNode.getMeasurements(),
                  insertTabletNode.getDataTypes(),
                  insertTabletNode.getColumns(),
                  start,
                  end);
        } else {
          memIncrements =
              checkMemCostAndAddToTspInfo(
                  insertTabletNode.getDevicePath().getFullPath(),
                  insertTabletNode.getMeasurements(),
                  insertTabletNode.getDataTypes(),
                  insertTabletNode.getColumns(),
                  start,
                  end);
        }
        PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemoryBlockCost(System.nanoTime() - startTime);
      }
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
      if (enableMemControl) {
        rollbackMemoryInfo(memIncrements);
      }
      throw new WriteProcessException(e);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordScheduleWalCost(System.nanoTime() - startTime);
    }

    startTime = System.nanoTime();

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
        insertTabletNode.getDeviceID().toStringID(), insertTabletNode.getTimes()[start]);
    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertTabletNode.getDeviceID().toStringID(), insertTabletNode.getTimes()[end - 1]);
    }

    tsFileResource.updateProgressIndex(insertTabletNode.getProgressIndex());

    PERFORMANCE_OVERVIEW_METRICS.recordScheduleMemTableCost(System.nanoTime() - startTime);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private long[] checkMemCostAndAddToTspInfo(
      String deviceId, String[] measurements, TSDataType[] dataTypes, Object[] values)
      throws WriteProcessException {
    // memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;
    // get device id
    IDeviceID deviceID = getDeviceID(deviceId);

    for (int i = 0; i < dataTypes.length; i++) {
      // skip failed Measurements
      if (dataTypes[i] == null || measurements[i] == null) {
        continue;
      }
      if (workMemTable.checkIfChunkDoesNotExist(deviceID, measurements[i])) {
        // ChunkMetadataIncrement
        chunkMetadataIncrement += ChunkMetadata.calculateRamSize(measurements[i], dataTypes[i]);
        memTableIncrement += TVList.tvListArrayMemCost(dataTypes[i]);
      } else {
        // here currentChunkPointNum >= 1
        long currentChunkPointNum = workMemTable.getCurrentTVListSize(deviceID, measurements[i]);
        memTableIncrement +=
            (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE) == 0
                ? TVList.tvListArrayMemCost(dataTypes[i])
                : 0;
      }
      // TEXT data mem size
      if (dataTypes[i] == TSDataType.TEXT && values[i] != null) {
        textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private long[] checkAlignedMemCostAndAddToTspInfo(
      String deviceId, String[] measurements, TSDataType[] dataTypes, Object[] values)
      throws WriteProcessException {
    // memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;
    // get device id
    IDeviceID deviceID = getDeviceID(deviceId);
    if (workMemTable.checkIfChunkDoesNotExist(deviceID, AlignedPath.VECTOR_PLACEHOLDER)) {
      // for new device of this mem table
      // ChunkMetadataIncrement
      chunkMetadataIncrement +=
          ChunkMetadata.calculateRamSize(AlignedPath.VECTOR_PLACEHOLDER, TSDataType.VECTOR)
              * dataTypes.length;
      memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypes);
      for (int i = 0; i < dataTypes.length; i++) {
        // skip failed Measurements
        if (dataTypes[i] == null || measurements[i] == null) {
          continue;
        }
        // TEXT data mem size
        if (dataTypes[i] == TSDataType.TEXT && values[i] != null) {
          textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
        }
      }
    } else {
      // for existed device of this mem table
      AlignedWritableMemChunk alignedMemChunk =
          ((AlignedWritableMemChunkGroup) workMemTable.getMemTableMap().get(deviceID))
              .getAlignedMemChunk();
      List<TSDataType> dataTypesInTVList = new ArrayList<>();
      for (int i = 0; i < dataTypes.length; i++) {
        // skip failed Measurements
        if (dataTypes[i] == null || measurements[i] == null) {
          continue;
        }

        // extending the column of aligned mem chunk
        if (!alignedMemChunk.containsMeasurement(measurements[i])) {
          memTableIncrement +=
              (alignedMemChunk.alignedListSize() / PrimitiveArrayManager.ARRAY_SIZE + 1)
                  * AlignedTVList.valueListArrayMemCost(dataTypes[i]);
          dataTypesInTVList.add(dataTypes[i]);
        }
        // TEXT data mem size
        if (dataTypes[i] == TSDataType.TEXT && values[i] != null) {
          textDataIncrement += MemUtils.getBinarySize((Binary) values[i]);
        }
      }
      // here currentChunkPointNum >= 1
      if ((alignedMemChunk.alignedListSize() % PrimitiveArrayManager.ARRAY_SIZE) == 0) {
        dataTypesInTVList.addAll(((AlignedTVList) alignedMemChunk.getTVList()).getTsDataTypes());
        memTableIncrement += AlignedTVList.alignedTvListArrayMemCost(dataTypesInTVList);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return new long[] {memTableIncrement, textDataIncrement, chunkMetadataIncrement};
  }

  private long[] checkMemCostAndAddToTspInfo(
      String deviceId,
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

    // get device id
    IDeviceID deviceID = getDeviceID(deviceId);

    for (int i = 0; i < dataTypes.length; i++) {
      // skip failed Measurements
      if (dataTypes[i] == null || columns[i] == null || measurements[i] == null) {
        continue;
      }
      updateMemCost(dataTypes[i], measurements[i], deviceID, start, end, memIncrements, columns[i]);
    }
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
    return memIncrements;
  }

  private long[] checkAlignedMemCostAndAddToTsp(
      String deviceId,
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

    // get device id
    IDeviceID deviceID = getDeviceID(deviceId);

    updateAlignedMemCost(dataTypes, deviceID, measurements, start, end, memIncrements, columns);
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
    if (dataType == TSDataType.TEXT) {
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
        if (dataType == TSDataType.TEXT) {
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
        // extending the column of aligned mem chunk
        if (!alignedMemChunk.containsMeasurement(measurementIds[i])) {
          memIncrements[0] +=
              (alignedMemChunk.alignedListSize() / PrimitiveArrayManager.ARRAY_SIZE + 1)
                  * AlignedTVList.valueListArrayMemCost(dataType);
          dataTypesInTVList.add(dataType);
        }
        // TEXT data size
        if (dataType == TSDataType.TEXT) {
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
      throws WriteProcessException {
    memTableIncrement += textDataIncrement;
    dataRegionInfo.addStorageGroupMemCost(memTableIncrement);
    tsFileProcessorInfo.addTSPMemCost(chunkMetadataIncrement);
    if (dataRegionInfo.needToReportToSystem()) {
      try {
        if (!SystemInfo.getInstance().reportStorageGroupStatus(dataRegionInfo, this)) {
          StorageEngine.blockInsertionIfReject(this);
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
      // flushing memTables are immutable, only record this deletion in these memTables for read
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
      logger.info(
          "The memtable size {} of tsfile {} reaches the mem control threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (!enableMemControl && workMemTable.memSize() >= getMemtableSizeThresholdBasedOnSeriesNum()) {
      logger.info(
          "The memtable size {} of tsfile {} reaches the threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (workMemTable.reachTotalPointNumThreshold()) {
      logger.info(
          "The avg series points num {} of tsfile {} reaches the threshold",
          workMemTable.getTotalPointsNum() / workMemTable.getSeriesNumber(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    return false;
  }

  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    return config.getMemtableSizeThreshold();
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getTsFileSize();
    long fileSizeThreshold = sequence ? config.getSeqTsFileSize() : config.getUnSeqTsFileSize();

    if (fileSize >= fileSizeThreshold) {
      logger.info(
          "{} fileSize {} >= fileSizeThreshold {}",
          tsFileResource.getTsFilePath(),
          fileSize,
          fileSizeThreshold);
    }
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
  public void asyncClose() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (logger.isInfoEnabled()) {
        if (workMemTable != null) {
          logger.info(
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
          logger.info(
              "{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              tsFileResource.getTsFileSize());
        }
      }

      if (shouldClose) {
        return;
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
        PipeAgent.runtime().assignSimpleProgressIndexIfNeeded(tsFileResource);
        PipeInsertionDataNodeListener.getInstance()
            .listenToTsFile(
                dataRegionInfo.getDataRegion().getDataRegionId(), tsFileResource, false);

        // When invoke closing TsFile after insert data to memTable, we shouldn't flush until invoke
        // flushing memTable in System module.
        addAMemtableIntoFlushingList(tmpMemTable);
        logger.info("Memtable {} has been added to flushing list", tmpMemTable);
        shouldClose = true;
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

  /** put the working memtable into flushing list and set the working memtable to null */
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
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    Map<String, Long> lastTimeForEachDevice = new HashMap<>();
    if (sequence) {
      lastTimeForEachDevice = tobeFlushed.getMaxTime();
      // If some devices have been removed in MemTable, the number of device in MemTable and
      // tsFileResource will not be the same. And the endTime of these devices in resource will be
      // Long.minValue.
      // In the case, we need to delete the removed devices in tsFileResource.
      if (lastTimeForEachDevice.size() != tsFileResource.getDevices().size()) {
        tsFileResource.deleteRemovedDeviceAndUpdateEndTime(lastTimeForEachDevice);
      } else {
        tsFileResource.updateEndTime(lastTimeForEachDevice);
      }
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onMemTableFlushStarted(tobeFlushed);
    }

    lastWorkMemtableFlushTime = System.currentTimeMillis();
    updateLatestFlushTimeCallback.call(this, lastTimeForEachDevice, lastWorkMemtableFlushTime);

    if (enableMemControl) {
      SystemInfo.getInstance().addFlushingMemTableCost(tobeFlushed.getTVListsRamCost());
    }
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
    workMemTable = null;
    FlushManager.getInstance().registerTsFileProcessor(this);
  }

  /** put back the memtable to MemTablePool and make metadata in writer visible */
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
      if (enableMemControl) {
        // reset the mem cost in StorageGroupProcessorInfo
        dataRegionInfo.releaseStorageGroupMemCost(memTable.getTVListsRamCost());
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[mem control] {}: {} flush finished, try to reset system memcost, "
                  + "flushing memtable list size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              flushingMemTables.size());
        }
        // report to System
        SystemInfo.getInstance().resetStorageGroupStatus(dataRegionInfo);
        SystemInfo.getInstance().resetFlushingMemTableCost(memTable.getTVListsRamCost());
      }
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

    // signal memtable only may appear when calling asyncClose()
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
            // release resource
            try {
              syncReleaseFlushedMemTable(memTableToFlush);
              // make sure no read will search this file
              tsFileResource.setTimeIndex(config.getTimeIndexLevel().getTimeIndex());
              // this callback method will register this empty tsfile into TsFileManager
              for (CloseFileListener closeFileListener : closeFileListeners) {
                closeFileListener.onClosed(this);
              }
              // close writer
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

    // for sync flush
    syncReleaseFlushedMemTable(memTableToFlush);

    // call flushed listener after memtable is released safely
    for (FlushListener flushListener : flushListeners) {
      flushListener.onMemTableFlushed(memTableToFlush);
    }
    // retry to avoid unnecessary read-only mode
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
        // truncate broken metadata
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error(
              "{}: {} truncate corrupted data meets error",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              e1);
        }
        // retry or set read-only
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
      // for sync close
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
          compressionRatio,
          totalMemTableSize,
          writer.getPos());
      String dataRegionId = dataRegionInfo.getDataRegion().getDataRegionId();
      WritingMetrics.getInstance()
          .recordTsFileCompressionRatioOfFlushingMemTable(dataRegionId, compressionRatio);
      CompressionRatio.getInstance().updateRatio(compressionRatio);
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
    logger.info("Start to end file {}", tsFileResource);
    long closeStartTime = System.currentTimeMillis();
    writer.endFile();
    tsFileResource.serialize();
    logger.info("Ended file {}", tsFileResource);

    // remove this processor from Closing list in StorageGroupProcessor,
    // mark the TsFileResource closed, no need writer anymore
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this);
    }

    if (enableMemControl) {
      tsFileProcessorInfo.clear();
      dataRegionInfo.closeTsFileProcessorAndReportToSystem(this);
    }
    if (logger.isInfoEnabled()) {
      long closeEndTime = System.currentTimeMillis();
      logger.info(
          "Database {} close the file {}, TsFile size is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName,
          tsFileResource.getTsFile().getAbsoluteFile(),
          writer.getFile().length(),
          closeEndTime - closeStartTime);
    }

    writer = null;
  }

  /** end empty file and remove it from file system */
  private void endEmptyFile() throws TsFileProcessorException, IOException {
    logger.info("Start to end empty file {}", tsFileResource);

    // remove this processor from Closing list in DataRegion,
    // mark the TsFileResource closed, no need writer anymore
    writer.close();
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this);
    }
    if (enableMemControl) {
      tsFileProcessorInfo.clear();
      dataRegionInfo.closeTsFileProcessorAndReportToSystem(this);
    }
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

  /** sync method */
  public boolean isMemtableNotNull() {
    flushQueryLock.writeLock().lock();
    try {
      return workMemTable != null;
    } finally {
      flushQueryLock.writeLock().unlock();
    }
  }

  /** close this tsfile */
  public void close() throws TsFileProcessorException {
    try {
      // when closing resource file, its corresponding mod file is also closed.
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

  /**
   * get the chunk(s) in the memtable (one from work memtable and the other ones in flushing
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
          for (IMemTable flushingMemTable : flushingMemTables) {
            if (flushingMemTable.isSignalMemTable()) {
              continue;
            }
            ReadOnlyMemChunk memChunk =
                flushingMemTable.query(
                    seriesPath, context.getQueryTimeLowerBound(), modsToMemtable);
            if (memChunk != null) {
              readOnlyMemChunks.add(memChunk);
            }
          }
          if (workMemTable != null) {
            ReadOnlyMemChunk memChunk =
                workMemTable.query(seriesPath, context.getQueryTimeLowerBound(), null);
            if (memChunk != null) {
              readOnlyMemChunks.add(memChunk);
            }
          }

          List<IChunkMetadata> chunkMetadataList =
              ResourceByPathUtils.getResourceInstance(seriesPath)
                  .getVisibleMetadataListFromWriter(writer, tsFileResource, context);

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

  public long getTimeRangeId() {
    return timeRangeId;
  }

  public void setTimeRangeId(long timeRangeId) {
    this.timeRangeId = timeRangeId;
  }

  /** release resource of a memtable */
  public void putMemTableBackAndClose() throws TsFileProcessorException {
    if (workMemTable != null) {
      workMemTable.release();
      workMemTable = null;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public TsFileProcessorInfo getTsFileProcessorInfo() {
    return tsFileProcessorInfo;
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

  public long getLastWorkMemtableFlushTime() {
    return lastWorkMemtableFlushTime;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void setWorkMemTableShouldFlush() {
    workMemTable.setShouldFlush();
  }

  public void addFlushListener(FlushListener listener) {
    flushListeners.add(listener);
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

  private IDeviceID getDeviceID(String deviceId) {
    return DeviceIDFactory.getInstance().getDeviceID(deviceId);
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
