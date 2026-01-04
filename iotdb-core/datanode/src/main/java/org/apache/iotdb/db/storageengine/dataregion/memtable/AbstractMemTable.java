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
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
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
import org.apache.iotdb.db.utils.EncryptDBUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractMemTable implements IMemTable {

  /** Each memTable node has a unique int value identifier, init when recovering wal. */
  public static final AtomicLong memTableIdCounter = new AtomicLong(-1);

  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + 2 * Integer.BYTES + 6 * Long.BYTES;

  /** DeviceId -> chunkGroup(MeasurementId -> chunk). */
  private final Map<IDeviceID, IWritableMemChunkGroup> memTableMap;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  private boolean shouldFlush = false;
  private volatile FlushStatus flushStatus = FlushStatus.WORKING;

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
        memTableMap.computeIfAbsent(
            deviceId,
            k ->
                new WritableMemChunkGroup(
                    EncryptDBUtils.getSecondEncryptParamFromDatabase(database)));
    for (IMeasurementSchema schema : schemaList) {
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementName())) {
        seriesNumber++;
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
              return new AlignedWritableMemChunkGroup(
                  schemaList.stream().filter(Objects::nonNull).collect(Collectors.toList()),
                  k.isTableModel());
            });
    for (IMeasurementSchema schema : schemaList) {
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementName())) {
        seriesNumber++;
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
            - (IoTDBDescriptor.getInstance().getConfig().isIncludeNullValueInWriteThroughputMetric()
                ? 0
                : nullPointsNumber);

    totalPointsNum += pointsInserted;
    return pointsInserted;
  }

  @Override
  public int insertAlignedRow(InsertRowNode insertRowNode) {

    String[] measurements = insertRowNode.getMeasurements();
    Object[] values = insertRowNode.getValues();
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    int nullPointsNumber = 0;
    for (int i = 0; i < insertRowNode.getMeasurements().length; i++) {
      // Use measurements[i] to ignore failed partial insert
      if (measurements[i] == null
          || values[i] == null
          || insertRowNode.getColumnCategories() != null
              && insertRowNode.getColumnCategories()[i] != TsTableColumnCategory.FIELD) {
        if (values[i] == null) {
          nullPointsNumber++;
        }
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
        insertRowNode.getMeasurementColumnCnt()
            - insertRowNode.getFailedMeasurementNumber()
            - (IoTDBDescriptor.getInstance().getConfig().isIncludeNullValueInWriteThroughputMetric()
                ? 0
                : nullPointsNumber);
    totalPointsNum += pointsInserted;
    return pointsInserted;
  }

  @Override
  public int insertTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException {
    try {
      int nullPointsNumber = computeTabletNullPointsNumber(insertTabletNode, start, end);
      writeTabletNode(insertTabletNode, start, end);
      memSize += MemUtils.getTabletSize(insertTabletNode, start, end);
      int pointsInserted =
          ((insertTabletNode.getDataTypes().length - insertTabletNode.getFailedMeasurementNumber())
                  * (end - start))
              - (IoTDBDescriptor.getInstance()
                      .getConfig()
                      .isIncludeNullValueInWriteThroughputMetric()
                  ? 0
                  : nullPointsNumber);
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
      int nullPointsNumber = computeTabletNullPointsNumber(insertTabletNode, start, end);
      writeAlignedTablet(insertTabletNode, start, end, results);
      // TODO-Table: what is the relation between this and TsFileProcessor.checkMemCost
      memSize += MemUtils.getAlignedTabletSize(insertTabletNode, start, end, results);
      int pointsInserted =
          ((insertTabletNode.getMeasurementColumnCnt()
                      - insertTabletNode.getFailedMeasurementNumber())
                  * (end - start))
              - (IoTDBDescriptor.getInstance()
                      .getConfig()
                      .isIncludeNullValueInWriteThroughputMetric()
                  ? 0
                  : nullPointsNumber);
      totalPointsNum += pointsInserted;
      return pointsInserted;
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  private static int computeTabletNullPointsNumber(
      InsertTabletNode insertTabletNode, int start, int end) {
    Object[] values = insertTabletNode.getBitMaps();
    int nullPointsNumber = 0;
    if (values != null) {
      for (int i = 0; i < insertTabletNode.getMeasurements().length; i++) {
        BitMap bitMap = (BitMap) values[i];
        if (bitMap != null && !bitMap.isAllUnmarked()) {
          for (int j = start; j < end; j++) {
            if (bitMap.isMarked(j)) {
              nullPointsNumber++;
            }
          }
        }
      }
    }
    return nullPointsNumber;
  }

  @Override
  public void write(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.writeRow(insertTime, objectValue, schemaList);
  }

  @Override
  public void writeAlignedRow(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.writeRow(insertTime, objectValue, schemaList);
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
    memChunkGroup.writeTablet(
        insertTabletNode.getTimes(),
        insertTabletNode.getColumns(),
        insertTabletNode.getBitMaps(),
        schemaList,
        start,
        end,
        null);
  }

  public void writeAlignedTablet(
      InsertTabletNode insertTabletNode, int start, int end, TSStatus[] results) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletNode.getMeasurementSchemas().length; i++) {
      if (insertTabletNode.getColumns()[i] == null
          || (insertTabletNode.getColumnCategories() != null
              && insertTabletNode.getColumnCategories()[i] != TsTableColumnCategory.FIELD)) {
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
      memChunkGroup.writeTablet(
          insertTabletNode.getTimes(),
          insertTabletNode.getColumns(),
          insertTabletNode.getBitMaps(),
          schemaList,
          splitStart,
          splitEnd,
          results);
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
  public IWritableMemChunk getWritableMemChunk(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return null;
    }
    return memChunkGroup.getWritableMemChunk(measurement);
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
      List<Pair<ModEntry, IMemTable>> modsToMemtable,
      Filter globalTimeFilter)
      throws IOException, QueryProcessException {
    return ResourceByPathUtils.getResourceInstance(fullPath)
        .getReadOnlyMemChunkFromMemTable(
            context, this, modsToMemtable, ttlLowerBound, globalTimeFilter);
  }

  @Override
  public void queryForSeriesRegionScan(
      IFullPath fullPath,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetaDataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled,
      Filter globalTimeFilter) {

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
          deviceID,
          measurementId,
          chunkMetaDataMap,
          memChunkHandleMap,
          deletionList,
          globalTimeFilter);
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
          deletionList,
          globalTimeFilter);
    }
  }

  @Override
  public void queryForDeviceRegionScan(
      IDeviceID deviceID,
      boolean isAligned,
      long ttlLowerBound,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<Pair<ModEntry, IMemTable>> modsToMemTabled,
      Filter globalTimeFilter) {

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
          modsToMemTabled,
          globalTimeFilter);
    } else {
      getMemChunkHandleFromMemTable(
          deviceID,
          (WritableMemChunkGroup) writableMemChunkGroup,
          chunkMetadataMap,
          memChunkHandleMap,
          ttlLowerBound,
          modsToMemTabled,
          globalTimeFilter);
    }
  }

  private void getMemChunkHandleFromMemTable(
      IDeviceID deviceID,
      String measurementId,
      Map<String, List<IChunkMetadata>> chunkMetadataMap,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<TimeRange> deletionList,
      Filter globalTimeFilter) {

    WritableMemChunk memChunk =
        (WritableMemChunk) memTableMap.get(deviceID).getMemChunkMap().get(measurementId);

    if (memChunk == null) {
      return;
    }
    Optional<Long> anySatisfiedTimestamp =
        memChunk.getAnySatisfiedTimestamp(deletionList, globalTimeFilter);
    if (!anySatisfiedTimestamp.isPresent()) {
      return;
    }
    long satisfiedTimestamp = anySatisfiedTimestamp.get();

    chunkMetadataMap
        .computeIfAbsent(measurementId, k -> new ArrayList<>())
        .add(
            buildFakeChunkMetaDataForFakeMemoryChunk(
                measurementId, satisfiedTimestamp, satisfiedTimestamp, Collections.emptyList()));
    memChunkHandleMap
        .computeIfAbsent(measurementId, k -> new ArrayList<>())
        .add(new MemChunkHandleImpl(deviceID, measurementId, new long[] {satisfiedTimestamp}));
  }

  private void getMemAlignedChunkHandleFromMemTable(
      IDeviceID deviceID,
      List<IMeasurementSchema> schemaList,
      Map<String, List<IChunkMetadata>> chunkMetadataList,
      Map<String, List<IChunkHandle>> memChunkHandleMap,
      List<List<TimeRange>> deletionList,
      Filter globalTimeFilter) {

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

    List<BitMap> bitMaps = new ArrayList<>();
    long[] timestamps =
        alignedMemChunk.getAnySatisfiedTimestamp(deletionList, bitMaps, true, globalTimeFilter);
    if (timestamps.length == 0) {
      return;
    }

    buildAlignedMemChunkHandle(
        deviceID,
        timestamps,
        bitMaps,
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
      List<Pair<ModEntry, IMemTable>> modsToMemTabled,
      Filter globalTimeFilter) {

    AlignedWritableMemChunk memChunk = writableMemChunkGroup.getAlignedMemChunk();
    List<IMeasurementSchema> schemaList = memChunk.getSchemaList();

    List<List<TimeRange>> deletionList = new ArrayList<>();
    if (modsToMemTabled != null) {
      for (IMeasurementSchema schema : schemaList) {
        deletionList.add(
            ModificationUtils.constructDeletionList(
                deviceID, schema.getMeasurementName(), this, modsToMemTabled, ttlLowerBound));
      }
    }

    List<BitMap> bitMaps = new ArrayList<>();
    long[] timestamps =
        memChunk.getAnySatisfiedTimestamp(deletionList, bitMaps, true, globalTimeFilter);
    if (timestamps.length == 0) {
      return;
    }
    buildAlignedMemChunkHandle(
        deviceID,
        timestamps,
        bitMaps,
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
      List<Pair<ModEntry, IMemTable>> modsToMemTabled,
      Filter globalTimeFilter) {

    for (Entry<String, IWritableMemChunk> entry :
        writableMemChunkGroup.getMemChunkMap().entrySet()) {

      String measurementId = entry.getKey();
      WritableMemChunk writableMemChunk = (WritableMemChunk) entry.getValue();

      List<TimeRange> deletionList = new ArrayList<>();
      if (modsToMemTabled != null) {
        deletionList =
            ModificationUtils.constructDeletionList(
                deviceID, measurementId, this, modsToMemTabled, ttlLowerBound);
      }
      Optional<Long> anySatisfiedTimestamp =
          writableMemChunk.getAnySatisfiedTimestamp(deletionList, globalTimeFilter);
      if (!anySatisfiedTimestamp.isPresent()) {
        return;
      }
      long satisfiedTimestamp = anySatisfiedTimestamp.get();
      chunkMetadataMap
          .computeIfAbsent(measurementId, k -> new ArrayList<>())
          .add(
              buildFakeChunkMetaDataForFakeMemoryChunk(
                  measurementId, satisfiedTimestamp, satisfiedTimestamp, Collections.emptyList()));
      memChunkHandleMap
          .computeIfAbsent(measurementId, k -> new ArrayList<>())
          .add(new MemChunkHandleImpl(deviceID, measurementId, new long[] {satisfiedTimestamp}));
    }
  }

  private void buildAlignedMemChunkHandle(
      IDeviceID deviceID,
      long[] timestamps,
      List<BitMap> bitMaps,
      List<List<TimeRange>> deletionList,
      List<IMeasurementSchema> schemaList,
      Map<String, List<IChunkMetadata>> chunkMetadataList,
      Map<String, List<IChunkHandle>> chunkHandleMap) {

    for (int column = 0; column < schemaList.size(); column++) {
      String measurement = schemaList.get(column).getMeasurementName();
      List<TimeRange> deletion =
          deletionList == null || deletionList.isEmpty()
              ? Collections.emptyList()
              : deletionList.get(column);

      long[] startEndTime = calculateStartEndTime(timestamps, bitMaps, column);
      chunkMetadataList
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(
              buildFakeChunkMetaDataForFakeMemoryChunk(
                  measurement, startEndTime[0], startEndTime[1], deletion));
      chunkHandleMap
          .computeIfAbsent(measurement, k -> new ArrayList<>())
          .add(
              new MemAlignedChunkHandleImpl(
                  deviceID, measurement, timestamps, bitMaps, column, startEndTime));
    }
  }

  private long[] calculateStartEndTime(long[] timestamps, List<BitMap> bitMaps, int column) {
    if (bitMaps.isEmpty()) {
      return new long[] {timestamps[0], timestamps[timestamps.length - 1]};
    }
    long startTime = -1, endTime = -1;
    for (int i = 0; i < timestamps.length; i++) {
      if (!bitMaps.get(i).isMarked(column)) {
        startTime = timestamps[i];
        break;
      }
    }

    for (int i = timestamps.length - 1; i >= 0; i--) {
      if (!bitMaps.get(i).isMarked(column)) {
        endTime = timestamps[i];
        break;
      }
    }
    return new long[] {startTime, endTime};
  }

  private IChunkMetadata buildFakeChunkMetaDataForFakeMemoryChunk(
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
      if (pair.right.isEmpty()) {
        memTableMap.remove(pair.left).release();
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
    if (isSignalMemTable()) {
      WALWriteUtils.write(isSignalMemTable(), buffer);
      return;
    }
    // marker for multi-tvlist mem chunk
    WALWriteUtils.write((byte) -1, buffer);
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

  protected void deserialize(DataInputStream stream, boolean multiTvListInMemChunk)
      throws IOException {
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
      if (multiTvListInMemChunk) {
        if (isAligned) {
          memChunkGroup = AlignedWritableMemChunkGroup.deserialize(stream, deviceID.isTableModel());
        } else {
          memChunkGroup = WritableMemChunkGroup.deserialize(stream);
        }
      } else {
        if (isAligned) {
          memChunkGroup =
              AlignedWritableMemChunkGroup.deserializeSingleTVListMemChunks(
                  stream, deviceID.isTableModel());
        } else {
          memChunkGroup = WritableMemChunkGroup.deserializeSingleTVListMemChunks(stream);
        }
      }
      memTableMap.put(deviceID, memChunkGroup);
    }
  }

  public void deserializeFromOldMemTableSnapshot(
      DataInputStream stream, boolean multiTvListInMemChunk) throws IOException {
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
      if (multiTvListInMemChunk) {
        if (isAligned) {
          memChunkGroup = AlignedWritableMemChunkGroup.deserialize(stream, deviceID.isTableModel());
        } else {
          memChunkGroup = WritableMemChunkGroup.deserialize(stream);
        }
      } else {
        if (isAligned) {
          memChunkGroup =
              AlignedWritableMemChunkGroup.deserializeSingleTVListMemChunks(
                  stream, deviceID.isTableModel());
        } else {
          memChunkGroup = WritableMemChunkGroup.deserializeSingleTVListMemChunks(stream);
        }
      }
      memTableMap.put(deviceID, memChunkGroup);
    }
  }

  @Override
  public Map<IDeviceID, Long> getMaxTime() {
    Map<IDeviceID, Long> latestTimeForEachDevice = new HashMap<>();
    for (Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      if (entry.getValue().count() > 0 && !entry.getValue().isEmpty()) {
        latestTimeForEachDevice.put(entry.getKey(), entry.getValue().getMaxTime());
      }
    }
    return latestTimeForEachDevice;
  }

  public static class Factory {

    private Factory() {
      // Empty constructor
    }

    public static IMemTable create(DataInputStream stream) throws IOException {
      byte marker = ReadWriteIOUtils.readByte(stream);
      boolean isSignal = marker == 1;
      IMemTable memTable;
      if (isSignal) {
        memTable = new NotifyFlushMemTable();
      } else {
        // database will be updated when deserialize
        PrimitiveMemTable primitiveMemTable = new PrimitiveMemTable();
        primitiveMemTable.deserialize(stream, marker == -1);
        memTable = primitiveMemTable;
      }
      return memTable;
    }

    public static IMemTable createFromOldMemTableSnapshot(DataInputStream stream)
        throws IOException {
      byte marker = ReadWriteIOUtils.readByte(stream);
      boolean isSignal = marker == 1;
      IMemTable memTable;
      if (isSignal) {
        memTable = new NotifyFlushMemTable();
      } else {
        // database will be updated when deserialize
        PrimitiveMemTable primitiveMemTable = new PrimitiveMemTable();
        primitiveMemTable.deserializeFromOldMemTableSnapshot(stream, marker == -1);
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
    for (IWritableMemChunkGroup memChunkGroup : memTableMap.values()) {
      memChunkGroup.setEncryptParameter(EncryptDBUtils.getSecondEncryptParamFromDatabase(database));
    }
  }

  @Override
  public void checkDataType(InsertNode node) throws DataTypeInconsistentException {
    node.checkDataType(this);
  }

  public IWritableMemChunkGroup getWritableMemChunkGroup(IDeviceID deviceID) {
    return memTableMap.get(deviceID);
  }
}
