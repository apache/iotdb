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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;

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

  Map<IDeviceID, IWritableMemChunkGroup> getMemTableMap();

  void write(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue);

  void writeAlignedRow(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue);

  /**
   * @return the number of points
   */
  long size();

  /**
   * @return memory usage
   */
  long memSize();

  /** only used when mem control enabled */
  void addTVListRamCost(long cost);

  /** only used when mem control enabled */
  void releaseTVListRamCost(long cost);

  /** only used when mem control enabled */
  long getTVListsRamCost();

  boolean reachChunkSizeOrPointNumThreshold();

  int getSeriesNumber();

  long getTotalPointsNum();

  /**
   * insert into this memtable
   *
   * @param insertRowNode insertRowNode
   */
  void insert(InsertRowNode insertRowNode);

  void insertAlignedRow(InsertRowNode insertRowNode);

  /**
   * insert tablet into this memtable. The rows to be inserted are in the range [start, end). Null
   * value in each column values will be replaced by the subsequent non-null value, e.g., {1, null,
   * 3, null, 5} will be {1, 3, 5, null, 5}
   *
   * @param insertTabletNode insertTabletNode
   * @param start included
   * @param end excluded
   */
  void insertTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException;

  void insertAlignedTablet(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results)
      throws WriteProcessException;

  ReadOnlyMemChunk query(
      QueryContext context,
      IFullPath fullPath,
      long ttlLowerBound,
      List<Pair<Modification, IMemTable>> modsToMemtabled)
      throws IOException, QueryProcessException, MetadataException;

  void queryForSeriesRegionScan(
      IFullPath fullPath,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<Modification, IMemTable>> modsToMemtabled)
      throws IOException, QueryProcessException, MetadataException;

  void queryForDeviceRegionScan(
      IDeviceID deviceID,
      boolean isAligned,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<Modification, IMemTable>> modsToMemtabled)
      throws IOException, QueryProcessException, MetadataException;

  /** putBack all the memory resources. */
  void clear();

  boolean isEmpty();

  /**
   * Delete data in it whose timestamp <= 'timestamp' and belonging to timeseries path. Only called
   * for non-flushing MemTable.
   *
   * @param path the PartialPath the timeseries to be deleted.
   * @param devicePath the device path of the timeseries to be deleted.
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

  boolean isSignalMemTable();

  void setShouldFlush();

  boolean shouldFlush();

  /** release resource of this memtable */
  void release();

  /** must guarantee the device exists in the work memtable only used when mem control enabled */
  boolean chunkNotExist(IDeviceID deviceId, String measurement);

  /** only used when mem control enabled */
  long getCurrentTVListSize(IDeviceID deviceId, String measurement);

  /** only used when mem control enabled */
  void addTextDataSize(long textDataIncrement);

  /** only used when mem control enabled */
  void releaseTextDataSize(long textDataDecrement);

  long getMaxPlanIndex();

  long getMinPlanIndex();

  long getMemTableId();

  long getCreatedTime();

  long getUpdateTime();

  FlushStatus getFlushStatus();

  void setFlushStatus(FlushStatus flushStatus);

  Map<IDeviceID, Long> getMaxTime();

  String getDatabase();

  String getDataRegionId();

  void setDatabaseAndDataRegionId(String database, String dataRegionId);

  void markAsNotGeneratedByPipe();

  boolean isTotallyGeneratedByPipe();
}
