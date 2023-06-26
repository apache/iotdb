package org.apache.iotdb.db.engine.memtable;

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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.flush.FlushStatus;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of
 * IMemTable maintains all series belonging to one StorageGroup, corresponding to one
 * StorageGroupProcessor.<br>
 * The concurrent control of IMemTable is based on the concurrent control of StorageGroupProcessor,
 * i.e., Writing and querying operations must already have gotten writeLock and readLock
 * respectively.<br>
 */
public interface IMemTable extends WALEntryValue {

  /**
   * Returns the memTable map collection.
   *
   * @return DeviceId -> chunkGroup(MeasurementId -> chunk).
   */
  Map<IDeviceID, IWritableMemChunkGroup> getMemTableMap();

  /**
   * Writes data to memTable.
   *
   * @param deviceId deviceId of memTable
   * @param schemaList schema of deviceId
   * @param insertTime timestamp of deviceId
   * @param objectValue values of deviceId
   */
  void write(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue);

  /**
   * Writes align data to memTable.
   *
   * @param deviceId deviceId of memTable
   * @param schemaList schema of deviceId
   * @param insertTime timestamp of deviceId
   * @param objectValue values of deviceId
   */
  void writeAlignedRow(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue);

  /**
   * The size of memTable.
   *
   * @return the number of points
   */
  long size();

  /**
   * Memory size of data points.
   *
   * @return memory usage
   */
  long memSize();

  /**
   * Adds TVList memory size,only used when mem control enabled.
   *
   * @param cost the cost byte of memory is to be consumed.
   */
  void addTvListRamCost(long cost);

  /**
   * Release TVList memory size,only used when mem control enabled.
   *
   * @param cost the cost byte of memory is to be released.
   */
  void releaseTvListRamCost(long cost);

  /**
   * Memory consumed by the current TvList,only used when mem control enabled.
   *
   * @return TVList memory usage.
   */
  long getTvListsRamCost();

  /**
   * Check whether the number of memTable points reaches the threshold.
   *
   * @return true indicates reached,false indicates not reached.
   */
  boolean reachTotalPointNumThreshold();

  /**
   * Return time series number of memTable.
   *
   * @return time series number.
   */
  int getSeriesNumber();

  /**
   * Return total points numbers of memTable.
   *
   * @return total points numbers.
   */
  long getTotalPointsNum();

  /**
   * Insert into this memTable with data.
   *
   * @param insertRowNode insertRowNode.
   */
  void insert(InsertRowNode insertRowNode);

  /**
   * Insert into this memTable with align data.
   *
   * @param insertRowNode insertRowNode.
   */
  void insertAlignedRow(InsertRowNode insertRowNode);

  /**
   * Insert tablet into this memTable. The rows to be inserted are in the range [start, end). Null
   * value in each column values will be replaced by the subsequent non-null value, e.g., {1, null,
   * 3, null, 5} will be {1, 3, 5, null, 5} .
   *
   * @param insertTabletNode insertTabletNode
   * @param start included
   * @param end excluded
   * @throws WriteProcessException write exception when inserted.
   */
  void insertTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException;

  /**
   * Insert align tablet into this memTable. The rows to be inserted are in the range [start, end).
   * Null value in each column values will be replaced by the subsequent non-null value, e.g., {1,
   * null, 3, null, 5} will be {1, 3, 5, null, 5} .
   *
   * @param insertTabletNode insertTabletNode
   * @param start included
   * @param end excluded
   * @throws WriteProcessException write exception when inserted.
   */
  void insertAlignedTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException;

  /**
   * Query from memTable.
   *
   * @param fullPath fullPath.
   * @param ttlLowerBound ttlLowerBound.
   * @param modsToMemTable modsToMemTable.
   * @return ReadOnlyMemChunk.
   * @throws IOException IOException.
   * @throws QueryProcessException QueryProcessException.
   * @throws MetadataException MetadataException.
   */
  ReadOnlyMemChunk query(
      PartialPath fullPath, long ttlLowerBound, List<Pair<Modification, IMemTable>> modsToMemTable)
      throws IOException, QueryProcessException, MetadataException;

  /** PutBack all the memory resources. */
  void clear();

  /**
   * Check whether memTable is empty.
   *
   * @return true means empty,false means not empty.
   */
  boolean isEmpty();

  /**
   * Delete data in it whose timestamp <= 'timestamp' and belonging to time series path. Only called
   * for non-flushing MemTable.
   *
   * @param path the PartialPath the time series to be deleted.
   * @param devicePath the device path of the time series to be deleted.
   * @param startTimestamp the lower-bound of deletion time.
   * @param endTimestamp the upper-bound of deletion time
   */
  void delete(PartialPath path, PartialPath devicePath, long startTimestamp, long endTimestamp);

  /**
   * Make a copy of this MemTable.
   *
   * @return a MemTable with the same data as this one.
   */
  IMemTable copy();

  /**
   * Check is signal memTable.
   *
   * @return NotifyFlushMemTable return true,primitiveMemTable return false.
   */
  boolean isSignalMemTable();

  /** Marks memTable should flush. */
  void setShouldFlush();

  /**
   * Determine whether memTable should flush.
   *
   * @return true means should flush,false means should not flush.
   */
  boolean shouldFlush();

  /** Release resource of this memTable. */
  void release();

  /**
   * Must guarantee the device exists in the work memTable only used when mem control enabled.
   *
   * @param deviceId deviceId.
   * @param measurement measurement.
   * @return true means not exists,false means exists.
   */
  boolean checkIfChunkDoesNotExist(IDeviceID deviceId, String measurement);

  /**
   * Example Query the size of the TvList of a specified device and measurement point,only used when
   * mem control enabled.
   *
   * @param deviceId deviceId.
   * @param measurement measurement.
   * @return tvList size.
   */
  long getCurrentTvListSize(IDeviceID deviceId, String measurement);

  /**
   * Add text data size,only used when mem control enabled.
   *
   * @param textDataIncrement increment of text data.
   */
  void addTextDataSize(long textDataIncrement);

  /**
   * Release text data size,only used when mem control enabled.
   *
   * @param textDataDecrement decrement of text data.
   */
  void releaseTextDataSize(long textDataDecrement);

  /**
   * Get max plan index of memTable.
   *
   * @return max plan index.
   */
  long getMaxPlanIndex();

  /**
   * Get min plan index of memTable.
   *
   * @return min plan index.
   */
  long getMinPlanIndex();

  /**
   * Get ID of memTable.
   *
   * @return ID
   */
  long getMemTableId();

  /**
   * Get created time of memTable.
   *
   * @return create time.
   */
  long getCreatedTime();

  /**
   * Get flush status of memTable.
   *
   * @return flush status {@link FlushStatus}
   */
  FlushStatus getFlushStatus();

  /**
   * Set flush status of memTable.
   *
   * @param flushStatus flush Status{@link FlushStatus}
   */
  void setFlushStatus(FlushStatus flushStatus);

  /**
   * Get max time of memTable.
   *
   * @return max time.
   */
  Map<String, Long> getMaxTime();
}
