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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.ResourceByPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushStatus;
import org.apache.iotdb.db.storageengine.dataregion.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.MemAlignedChunkHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.MemChunkHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public abstract class AbstractMemTable implements IMemTable {

  /** Each memTable node has a unique int value identifier, init when recovering wal. */
  public static final AtomicLong memTableIdCounter = new AtomicLong(-1);

  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + 2 * Integer.BYTES + 6 * Long.BYTES;

  /** DeviceId -> chunkGroup(MeasurementId -> chunk). */
  private final Map<IDeviceID, IWritableMemChunkGroup> memTableMap;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  private boolean shouldFlush = false;
  private boolean reachChunkSizeOrPointNumThreshold = false;
  private volatile FlushStatus flushStatus = FlushStatus.WORKING;
  private final int avgSeriesPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();

  /** Memory size of data points, including TEXT values. */
  private long memSize = 0;

  /**
   * Memory usage of all TVLists memory usage regardless of whether these TVLists are full,
   * including TEXT values.
   */
  private long tvListRamCost = 0;

  private int seriesNumber = 0;

  private long totalPointsNum = 0;

  private long totalPointsNumThreshold = 0;

  private long maxPlanIndex = Long.MIN_VALUE;

  private long minPlanIndex = Long.MAX_VALUE;

  private final long memTableId = memTableIdCounter.incrementAndGet();

  private final long createdTime = System.currentTimeMillis();

  /** this time is updated by the timed flush, same as createdTime when the feature is disabled. */
  private long updateTime = createdTime;

  /**
   * check whether this memTable has been updated since last timed flush check, update updateTime
   * when changed
   */
  private long lastTotalPointsNum = totalPointsNum;

  private String database;
  private String dataRegionId;

  private static final String METRIC_POINT_IN = Metric.POINTS_IN.toString();

  private final AtomicBoolean isTotallyGeneratedByPipe = new AtomicBoolean(true);

  protected AbstractMemTable() {
    this.database = null;
    this.dataRegionId = null;
    this.memTableMap = new HashMap<>();
  }

  protected AbstractMemTable(String database, String dataRegionId) {
    this.database = database;
    this.dataRegionId = dataRegionId;
    this.memTableMap = new HashMap<>();
  }

  protected AbstractMemTable(
      String database, String dataRegionId, Map<IDeviceID, IWritableMemChunkGroup> memTableMap) {
    this.database = database;
    this.dataRegionId = dataRegionId;
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<IDeviceID, IWritableMemChunkGroup> getMemTableMap() {
    return memTableMap;
  }

  /**
   * Create this MemChunk if it's not exist.
   *
   * @param deviceId device id
   * @param schemaList measurement schemaList
   * @return this MemChunkGroup
   */
  private IWritableMemChunkGroup createMemChunkGroupIfNotExistAndGet(
      IDeviceID deviceId, List<IMeasurementSchema> schemaList) {
    IWritableMemChunkGroup memChunkGroup =
        memTableMap.computeIfAbsent(deviceId, k -> new WritableMemChunkGroup());
    for (IMeasurementSchema schema : schemaList) {
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementName())) {
        seriesNumber++;
        totalPointsNumThreshold += avgSeriesPointNumThreshold;
      }
    }
    return memChunkGroup;
  }

  private IWritableMemChunkGroup createAlignedMemChunkGroupIfNotExistAndGet(
      IDeviceID deviceId, List<IMeasurementSchema> schemaList) {
    IWritableMemChunkGroup memChunkGroup =
        memTableMap.computeIfAbsent(
            deviceId,
            k -> {
              seriesNumber += schemaList.size();
              totalPointsNumThreshold += ((long) avgSeriesPointNumThreshold) * schemaList.size();
              return new AlignedWritableMemChunkGroup(
                  schemaList.stream().filter(Objects::nonNull).collect(Collectors.toList()),
                  k.isTableModel());
            });
    for (IMeasurementSchema schema : schemaList) {
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementName())) {
        seriesNumber++;
        totalPointsNumThreshold += avgSeriesPointNumThreshold;
      }
    }
    return memChunkGroup;
  }

  @Override
  public int insert(InsertRowNode insertRowNode) {

    String[] measurements = insertRowNode.getMeasurements();
    Object[] values = insertRowNode.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    int nullPointsNumber = 0;
    for (int i = 0; i < insertRowNode.getMeasurements().length; i++) {
      // Use measurements[i] to ignore failed partial insert
      if (measurements[i] == null || values[i] == null) {
        if (values[i] == null) {
          nullPointsNumber++;
        }
        schemaList.add(null);
      } else {
        IMeasurementSchema schema = insertRowNode.getMeasurementSchemas()[i];
        schemaList.add(schema);
        dataTypes.add(schema.getType());
      }
    }
    memSize += MemUtils.getRowRecordSize(dataTypes, values);
    write(insertRowNode.getDeviceID(), schemaList, insertRowNode.getTime(), values);

    int pointsInserted =
        insertRowNode.getMeasurements().length
            - insertRowNode.getFailedMeasurementNumber()
            - nullPointsNumber;

    totalPointsNum += pointsInserted;
    return pointsInserted;
  }

  @Override
  public int insertAlignedRow(InsertRowNode insertRowNode) {

    String[] measurements = insertRowNode.getMeasurements();
    Object[] values = insertRowNode.getValues();
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < insertRowNode.getMeasurements().length; i++) {
      // Use measurements[i] to ignore failed partial insert
      if (measurements[i] == null
          || values[i] == null
          || insertRowNode.getColumnCategories() != null
              && insertRowNode.getColumnCategories()[i] != TsTableColumnCategory.MEASUREMENT) {
        schemaList.add(null);
        continue;
      }
      IMeasurementSchema schema = insertRowNode.getMeasurementSchemas()[i];
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    if (schemaList.isEmpty()) {
      return 0;
    }
    memSize +=
        MemUtils.getAlignedRowRecordSize(dataTypes, values, insertRowNode.getColumnCategories());
    writeAlignedRow(insertRowNode.getDeviceID(), schemaList, insertRowNode.getTime(), values);
    int pointsInserted =
        insertRowNode.getMeasurementColumnCnt() - insertRowNode.getFailedMeasurementNumber();
    totalPointsNum += pointsInserted;
    return pointsInserted;
  }

  @Override
  public int insertTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException {
    try {
      writeTabletNode(insertTabletNode, start, end);
      memSize += MemUtils.getTabletSize(insertTabletNode, start, end);
      int pointsInserted =
          (insertTabletNode.getDataTypes().length - insertTabletNode.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum += pointsInserted;
      return pointsInserted;
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public int insertAlignedTablet(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results)
      throws WriteProcessException {
    try {
      writeAlignedTablet(insertTabletNode, start, end, results);
      // TODO-Table: what is the relation between this and TsFileProcessor.checkMemCost
      memSize += MemUtils.getAlignedTabletSize(insertTabletNode, start, end, results);
      int pointsInserted =
          (insertTabletNode.getMeasurementColumnCnt()
                  - insertTabletNode.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum += pointsInserted;
      return pointsInserted;
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void write(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    if (memChunkGroup.writeWithFlushCheck(insertTime, objectValue, schemaList)) {
      reachChunkSizeOrPointNumThreshold = true;
    }
  }

  @Override
  public void writeAlignedRow(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    if (memChunkGroup.writeWithFlushCheck(insertTime, objectValue, schemaList)) {
      reachChunkSizeOrPointNumThreshold = true;
    }
  }

  public void writeTabletNode(InsertTabletNode insertTabletNode, int start, int end) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletNode.getMeasurementSchemas().length; i++) {
      if (insertTabletNode.getColumns()[i] == null) {
        schemaList.add(null);
      } else {
        schemaList.add(insertTabletNode.getMeasurementSchemas()[i]);
      }
    }
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(insertTabletNode.getDeviceID(), schemaList);
    if (memChunkGroup.writeValuesWithFlushCheck(
        insertTabletNode.getTimes(),
        insertTabletNode.getColumns(),
        insertTabletNode.getBitMaps(),
        schemaList,
        start,
        end,
        null)) {
      reachChunkSizeOrPointNumThreshold = true;
    }
  }

  public void writeAlignedTablet(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results) {

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletNode.getMeasurementSchemas().length; i++) {
      if (insertTabletNode.getColumns()[i] == null
          || (insertTabletNode.getColumnCategories() != null
              && insertTabletNode.getColumnCategories()[i] != TsTableColumnCategory.MEASUREMENT)) {
        schemaList.add(null);
      } else {
        schemaList.add(insertTabletNode.getMeasurementSchemas()[i]);
      }
    }
    if (schemaList.isEmpty()) {
      return;
    }
    final List<Pair<IDeviceID, Integer>> deviceEndOffsetPair =
        insertTabletNode.splitByDevice(start, end);
    int splitStart = start;
    for (Pair<IDeviceID, Integer> pair : deviceEndOffsetPair) {
      final IDeviceID deviceID = pair.left;
      int splitEnd = pair.right;
      IWritableMemChunkGroup memChunkGroup =
          createAlignedMemChunkGroupIfNotExistAndGet(deviceID, schemaList);
      if (memChunkGroup.writeValuesWithFlushCheck(
          insertTabletNode.getTimes(),
          insertTabletNode.getColumns(),
          insertTabletNode.getBitMaps(),
          schemaList,
          splitStart,
          splitEnd,
          results)) {
        reachChunkSizeOrPointNumThreshold = true;
      }
      splitStart = splitEnd;
    }
  }

  @Override
  public boolean chunkNotExist(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return true;
    }
    return !memChunkGroup.contains(measurement);
  }

  @Override
  public long getCurrentTVListSize(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return 0;
    }
    return memChunkGroup.getCurrentTVListSize(measurement);
  }

  @Override
  public int getSeriesNumber() {
    return seriesNumber;
  }

  @Override
  public long getTotalPointsNum() {
    return totalPointsNum;
  }

  @Override
  public long size() {
    long sum = 0;
    for (IWritableMemChunkGroup writableMemChunkGroup : memTableMap.values()) {
      sum += writableMemChunkGroup.count();
    }
    return sum;
  }

  @Override
  public long memSize() {
    return memSize;
  }

  @Override
  public boolean reachChunkSizeOrPointNumThreshold() {
    return reachChunkSizeOrPointNumThreshold;
  }

  @Override
  public void clear() {
    memTableMap.clear();
    memSize = 0;
    seriesNumber = 0;
    totalPointsNum = 0;
    totalPointsNumThreshold = 0;
    tvListRamCost = 0;
    maxPlanIndex = 0;
    minPlanIndex = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk query(
      QueryContext context,
      IFullPath fullPath,
      long ttlLowerBound,
      List<Pair<ModEntry, IMemTable>> modsToMemtable)
      throws IOException, QueryProcessException {
    return ResourceByPathUtils.getResourceInstance(fullPath)
        .getReadOnlyMemChunkFromMemTable(context, this, modsToMemtable, ttlLowerBound);
  }

  @Override
  public void queryForSeriesRegionScan(
      IFullPath fullPath,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetaDataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled) {

    IDeviceID deviceID = fullPath.getDeviceId();
    if (fullPath instanceof NonAlignedFullPath) {
      String measurementId = ((NonAlignedFullPath) fullPath).getMeasurement();

      // check If MemTable Contains this path
      if (!memTableMap.containsKey(deviceID)
          || !memTableMap.get(deviceID).contains(measurementId)) {
        return;
      }
      List<TimeRange> deletionList = new ArrayList<>();
      if (modsToMemTabled != null) {
        deletionList =
            ModificationUtils.constructDeletionList(
                fullPath.getDeviceId(), measurementId, this, modsToMemTabled, ttlLowerBound);
      }
      getMemChunkHandleFromMemTable(
          deviceID, measurementId, chunkMetaDataMap, memChunkHandleMap, deletionList);
    } else {
      // check If MemTable Contains this path
      if (!memTableMap.containsKey(deviceID)) {
        return;
      }
      List<List<TimeRange>> deletionList = new ArrayList<>();
      if (modsToMemTabled != null) {
        deletionList =
            ModificationUtils.constructDeletionList(
                deviceID,
                ((AlignedFullPath) fullPath).getMeasurementList(),
                this,
                modsToMemTabled,
                ttlLowerBound);
      }

      getMemAlignedChunkHandleFromMemTable(
          deviceID,
          ((AlignedFullPath) fullPath).getSchemaList(),
          chunkMetaDataMap,
          memChunkHandleMap,
          deletionList);
    }
  }

  @Override
  public void queryForDeviceRegionScan(
      IDeviceID deviceID,
      boolean isAligned,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled)
      throws MetadataException {

    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = getMemTableMap();

    // check If MemTable Contains this path
    if (!memTableMap.containsKey(deviceID)) {
      return;
    }

    IWritableMemChunkGroup writableMemChunkGroup = memTableMap.get(deviceID);
    if (isAligned) {
      getMemAlignedChunkHandleFromMemTable(
          deviceID,
          (AlignedWritableMemChunkGroup) writableMemChunkGroup,
          chunkMetadataMap,
          memChunkHandleMap,
          ttlLowerBound,
          modsToMemTabled);
    } else {
      getMemChunkHandleFromMemTable(
          deviceID,
          (WritableMemChunkGroup) writableMemChunkGroup,
          chunkMetadataMap,
          memChunkHandleMap,
          ttlLowerBound,
          modsToMemTabled);
    }
  }

  private void getMemChunkHandleFromMemTable(
      IDeviceID deviceID,
      String measurementId,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<TimeRange> deletionList) {

    IWritableMemChunk memChunk = memTableMap.get(deviceID).getMemChunkMap().get(measurementId);

    TVList tvListCopy = memChunk.getSortedTvListForQuery();
    long[] timestamps = filterDeletedTimestamp(tvListCopy, deletionList);

    chunkMetadataMap
        .computeIfAbsent(measurementId, k -> new ArrayList<>())
        .add(
            buildChunkMetaDataForMemoryChunk(
                measurementId,
                timestamps[0],
                timestamps[tvListCopy.rowCount() - 1],
                Collections.emptyList()));
    memChunkHandleMap
        .computeIfAbsent(measurementId, k -> new ArrayList<>())
        .add(new MemChunkHandleImpl(deviceID, measurementId, timestamps));
  }

  private void getMemAlignedChunkHandleFromMemTable(
      IDeviceID deviceID,
      List<IMeasurementSchema> schemaList,
      Map<String, List<IChunkMetadata>> chunkMetadataList,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<List<TimeRange>> deletionList) {

    AlignedWritableMemChunk alignedMemChunk =
        ((AlignedWritableMemChunkGroup) memTableMap.get(deviceID)).getAlignedMemChunk();

    boolean containsMeasurement = false;
    for (IMeasurementSchema measurementSchema : schemaList) {
      if (alignedMemChunk.containsMeasurement(measurementSchema.getMeasurementName())) {
        containsMeasurement = true;
        break;
      }
    }
    if (!containsMeasurement) {
      return;
    }

    AlignedTVList alignedTVListCopy =
        (AlignedTVList) alignedMemChunk.getSortedTvListForQuery(schemaList, true);

    buildAlignedMemChunkHandle(
        deviceID,
        alignedTVListCopy,
        deletionList,
        schemaList,
        chunkMetadataList,
        memChunkHandleMap);
  }

  private void getMemAlignedChunkHandleFromMemTable(
      IDeviceID deviceID,
      AlignedWritableMemChunkGroup writableMemChunkGroup,
      Map<String, List<IChunkMetadata>> chunkMetadataList,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      long ttlLowerBound,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled) {

    AlignedWritableMemChunk memChunk = writableMemChunkGroup.getAlignedMemChunk();
    List<IMeasurementSchema> schemaList = memChunk.getSchemaList();

    AlignedTVList alignedTVListCopy =
        (AlignedTVList) memChunk.getSortedTvListForQuery(schemaList, true);

    List<List<TimeRange>> deletionList = new ArrayList<>();
    if (modsToMemTabled != null) {
      for (IMeasurementSchema schema : schemaList) {
        deletionList.add(
            ModificationUtils.constructDeletionList(
                deviceID, schema.getMeasurementName(), this, modsToMemTabled, ttlLowerBound));
      }
    }
    buildAlignedMemChunkHandle(
        deviceID,
        alignedTVListCopy,
        deletionList,
        schemaList,
        chunkMetadataList,
        memChunkHandleMap);
  }

  private void getMemChunkHandleFromMemTable(
      IDeviceID deviceID,
      WritableMemChunkGroup writableMemChunkGroup,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      long ttlLowerBound,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled) {

    for (Entry<String, IWritableMemChunk> entry :
        writableMemChunkGroup.getMemChunkMap().entrySet()) {

      String measurementId = entry.getKey();
      IWritableMemChunk writableMemChunk = entry.getValue();
      TVList tvListCopy = writableMemChunk.getSortedTvListForQuery();

      List<TimeRange> deletionList = new ArrayList<>();
      if (modsToMemTabled != null) {
        deletionList =
            ModificationUtils.constructDeletionList(
                deviceID, measurementId, this, modsToMemTabled, ttlLowerBound);
      }
      long[] timestamps = filterDeletedTimestamp(tvListCopy, deletionList);
      chunkMetadataMap
          .computeIfAbsent(measurementId, k -> new ArrayList<>())
          .add(
              buildChunkMetaDataForMemoryChunk(
                  measurementId,
                  timestamps[0],
                  timestamps[tvListCopy.rowCount() - 1],
                  Collections.emptyList()));
      memChunkHandleMap
          .computeIfAbsent(measurementId, k -> new ArrayList<>())
          .add(new MemChunkHandleImpl(deviceID, measurementId, timestamps));
    }
  }

  private void buildAlignedMemChunkHandle(
      IDeviceID deviceID,
      AlignedTVList alignedTVList,
      List<List<TimeRange>> deletionList,
      List<IMeasurementSchema> schemaList,
      Map<String, List<IChunkMetadata>> chunkMetadataList,
      Map<String, List<IChunkHandle>> chunkHandleMap) {

    List<List<BitMap>> bitMaps = alignedTVList.getBitMaps();
    long[] timestamps =
        alignedTVList.getTimestamps().stream().flatMapToLong(LongStream::of).toArray();
    timestamps = Arrays.copyOfRange(timestamps, 0, alignedTVList.rowCount());

    for (int i = 0; i < schemaList.size(); i++) {
      String measurement = schemaList.get(i).getMeasurementName();
      List<BitMap> curBitMap = bitMaps == null ? Collections.emptyList() : bitMaps.get(i);
      List<TimeRange> deletion =
          deletionList == null || deletionList.isEmpty()
              ? Collections.emptyList()
              : deletionList.get(i);

      long[] startEndTime = calculateStartEndTime(timestamps, curBitMap);
      chunkMetadataList
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(
              buildChunkMetaDataForMemoryChunk(
                  measurement, startEndTime[0], startEndTime[1], deletion));
      chunkHandleMap
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(
              new MemAlignedChunkHandleImpl(
                  deviceID, measurement, timestamps, curBitMap, deletion, startEndTime));
    }
  }

  private long[] calculateStartEndTime(long[] timestamps, List<BitMap> bitMaps) {
    if (bitMaps.isEmpty()) {
      return new long[] {timestamps[0], timestamps[timestamps.length - 1]};
    }
    long startTime = -1, endTime = -1;
    for (int i = 0; i < timestamps.length; i++) {
      int arrayIndex = i / ARRAY_SIZE;
      int elementIndex = i % ARRAY_SIZE;
      if (!bitMaps.get(arrayIndex).isMarked(elementIndex)) {
        startTime = timestamps[i];
        break;
      }
    }

    for (int i = timestamps.length - 1; i >= 0; i--) {
      int arrayIndex = i / ARRAY_SIZE;
      int elementIndex = i % ARRAY_SIZE;
      if (!bitMaps.get(arrayIndex).isMarked(elementIndex)) {
        endTime = timestamps[i];
        break;
      }
    }
    return new long[] {startTime, endTime};
  }

  private IChunkMetadata buildChunkMetaDataForMemoryChunk(
      String measurement, long startTime, long endTime, List<TimeRange> deletionList) {
    TimeStatistics timeStatistics = new TimeStatistics();
    timeStatistics.setStartTime(startTime);
    timeStatistics.setEndTime(endTime);

    // ChunkMetaData for memory is only used to get time statistics, the dataType is irrelevant.
    IChunkMetadata chunkMetadata =
        new ChunkMetadata(
            measurement,
            TSDataType.UNKNOWN,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            0,
            timeStatistics);
    for (TimeRange timeRange : deletionList) {
      chunkMetadata.insertIntoSortedDeletions(timeRange);
    }
    return chunkMetadata;
  }

  private long[] filterDeletedTimestamp(TVList tvList, List<TimeRange> deletionList) {
    if (deletionList.isEmpty()) {
      long[] timestamps = tvList.getTimestamps().stream().flatMapToLong(LongStream::of).toArray();
      return Arrays.copyOfRange(timestamps, 0, tvList.rowCount());
    }

    long lastTime = -1;
    int[] deletionCursor = {0};
    int rowCount = tvList.rowCount();
    List<Long> result = new ArrayList<>();

    for (int i = 0; i < rowCount; i++) {
      long curTime = tvList.getTime(i);
      if (!ModificationUtils.isPointDeleted(curTime, deletionList, deletionCursor)
          && (i == rowCount - 1 || curTime != lastTime)) {
        result.add(curTime);
      }
      lastTime = curTime;
    }
    return result.stream().mapToLong(Long::longValue).toArray();
  }

  @Override
  public long delete(ModEntry modEntry) {
    List<Pair<IDeviceID, IWritableMemChunkGroup>> targetDeviceList = new ArrayList<>();
    for (Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      if (modEntry.affects(entry.getKey())) {
        targetDeviceList.add(new Pair<>(entry.getKey(), entry.getValue()));
      }
    }

    long pointDeleted = 0;
    for (Pair<IDeviceID, IWritableMemChunkGroup> pair : targetDeviceList) {
      if (modEntry.affectsAll(pair.left)) {
        pointDeleted += pair.right.deleteTime(modEntry);
      } else {
        pointDeleted += pair.right.delete(modEntry);
      }
      if (pair.right.getMemChunkMap().isEmpty()) {
        memTableMap.remove(pair.left);
      }
    }
    return pointDeleted;
  }

  @Override
  public void addTVListRamCost(long cost) {
    this.tvListRamCost += cost;
  }

  @Override
  public void releaseTVListRamCost(long cost) {
    this.tvListRamCost -= cost;
  }

  @Override
  public long getTVListsRamCost() {
    return tvListRamCost;
  }

  @Override
  public void addTextDataSize(long textDataSize) {
    this.memSize += textDataSize;
  }

  @Override
  public void releaseTextDataSize(long textDataSize) {
    this.memSize -= textDataSize;
  }

  @Override
  public void setShouldFlush() {
    shouldFlush = true;
  }

  @Override
  public boolean shouldFlush() {
    return shouldFlush;
  }

  @Override
  public void release() {
    for (Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      entry.getValue().release();
    }
  }

  @Override
  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  @Override
  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  @Override
  public long getMemTableId() {
    return memTableId;
  }

  @Override
  public long getCreatedTime() {
    return createdTime;
  }

  /** Check whether updated since last get method */
  @Override
  public long getUpdateTime() {
    if (lastTotalPointsNum != totalPointsNum) {
      lastTotalPointsNum = totalPointsNum;
      updateTime = System.currentTimeMillis();
    }
    return updateTime;
  }

  @Override
  public FlushStatus getFlushStatus() {
    return flushStatus;
  }

  @Override
  public void setFlushStatus(FlushStatus flushStatus) {
    this.flushStatus = flushStatus;
  }

  /** Notice: this method is concurrent unsafe. */
  @Override
  public int serializedSize() {
    if (isSignalMemTable()) {
      return Byte.BYTES;
    }
    int size = FIXED_SERIALIZED_SIZE;
    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      size += entry.getKey().serializedSize();
      size += Byte.BYTES;
      size += entry.getValue().serializedSize();
    }
    return size;
  }

  /** Notice: this method is concurrent unsafe. */
  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    // TODO:[WAL]
    WALWriteUtils.write(isSignalMemTable(), buffer);
    if (isSignalMemTable()) {
      return;
    }
    buffer.putInt(seriesNumber);
    buffer.putLong(memSize);
    buffer.putLong(tvListRamCost);
    buffer.putLong(totalPointsNum);
    buffer.putLong(totalPointsNumThreshold);
    buffer.putLong(maxPlanIndex);
    buffer.putLong(minPlanIndex);

    buffer.putInt(memTableMap.size());
    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      try {
        entry.getKey().serialize(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      IWritableMemChunkGroup memChunkGroup = entry.getValue();
      WALWriteUtils.write(memChunkGroup instanceof AlignedWritableMemChunkGroup, buffer);
      memChunkGroup.serializeToWAL(buffer);
    }
  }

  public void deserialize(DataInputStream stream) throws IOException {
    seriesNumber = stream.readInt();
    memSize = stream.readLong();
    tvListRamCost = stream.readLong();
    totalPointsNum = stream.readLong();
    totalPointsNumThreshold = stream.readLong();
    maxPlanIndex = stream.readLong();
    minPlanIndex = stream.readLong();

    int memTableMapSize = stream.readInt();
    for (int i = 0; i < memTableMapSize; ++i) {
      IDeviceID deviceID = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(stream);
      boolean isAligned = ReadWriteIOUtils.readBool(stream);
      IWritableMemChunkGroup memChunkGroup;
      if (isAligned) {
        memChunkGroup = AlignedWritableMemChunkGroup.deserialize(stream, deviceID.isTableModel());
      } else {
        memChunkGroup = WritableMemChunkGroup.deserialize(stream);
      }
      memTableMap.put(deviceID, memChunkGroup);
    }
  }

  public void deserializeFromOldMemTableSnapshot(DataInputStream stream) throws IOException {
    seriesNumber = stream.readInt();
    memSize = stream.readLong();
    tvListRamCost = stream.readLong();
    totalPointsNum = stream.readLong();
    totalPointsNumThreshold = stream.readLong();
    maxPlanIndex = stream.readLong();
    minPlanIndex = stream.readLong();

    int memTableMapSize = stream.readInt();
    for (int i = 0; i < memTableMapSize; ++i) {
      PartialPath devicePath;
      try {
        devicePath =
            DataNodeDevicePathCache.getInstance()
                .getPartialPath(ReadWriteIOUtils.readString(stream));
      } catch (IllegalPathException e) {
        throw new IllegalArgumentException("Cannot deserialize OldMemTableSnapshot", e);
      }
      IDeviceID deviceID = deviceIDFactory.getDeviceID(devicePath);
      boolean isAligned = ReadWriteIOUtils.readBool(stream);
      IWritableMemChunkGroup memChunkGroup;
      if (isAligned) {
        memChunkGroup = AlignedWritableMemChunkGroup.deserialize(stream, false);
      } else {
        memChunkGroup = WritableMemChunkGroup.deserialize(stream);
      }
      memTableMap.put(deviceID, memChunkGroup);
    }
  }

  @Override
  public Map<IDeviceID, Long> getMaxTime() {
    Map<IDeviceID, Long> latestTimeForEachDevice = new HashMap<>();
    for (Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      // When insert null values in to IWritableMemChunkGroup, the maxTime will not be updated.
      // In this scenario, the maxTime will be Long.MIN_VALUE. We shouldn't return this device.
      long maxTime = entry.getValue().getMaxTime();
      if (maxTime != Long.MIN_VALUE) {
        latestTimeForEachDevice.put(entry.getKey(), maxTime);
      }
    }
    return latestTimeForEachDevice;
  }

  public static class Factory {

    private Factory() {
      // Empty constructor
    }

    public static IMemTable create(DataInputStream stream) throws IOException {
      boolean isSignal = ReadWriteIOUtils.readBool(stream);
      IMemTable memTable;
      if (isSignal) {
        memTable = new NotifyFlushMemTable();
      } else {
        // database will be updated when deserialize
        PrimitiveMemTable primitiveMemTable = new PrimitiveMemTable();
        primitiveMemTable.deserialize(stream);
        memTable = primitiveMemTable;
      }
      return memTable;
    }

    public static IMemTable createFromOldMemTableSnapshot(DataInputStream stream)
        throws IOException {
      boolean isSignal = ReadWriteIOUtils.readBool(stream);
      IMemTable memTable;
      if (isSignal) {
        memTable = new NotifyFlushMemTable();
      } else {
        // database will be updated when deserialize
        PrimitiveMemTable primitiveMemTable = new PrimitiveMemTable();
        primitiveMemTable.deserializeFromOldMemTableSnapshot(stream);
        memTable = primitiveMemTable;
      }
      return memTable;
    }
  }

  @Override
  public String getDatabase() {
    return database;
  }

  @Override
  public String getDataRegionId() {
    return dataRegionId;
  }

  @Override
  public void setDatabaseAndDataRegionId(String database, String dataRegionId) {
    this.database = database;
    this.dataRegionId = dataRegionId;
  }

  @Override
  public void markAsNotGeneratedByPipe() {
    this.isTotallyGeneratedByPipe.set(false);
  }

  @Override
  public boolean isTotallyGeneratedByPipe() {
    return this.isTotallyGeneratedByPipe.get();
  }
}
