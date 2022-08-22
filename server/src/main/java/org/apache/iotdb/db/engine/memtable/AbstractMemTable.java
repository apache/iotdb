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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushStatus;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.deviceID.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.utils.ResourceByPathUtils;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractMemTable implements IMemTable {
  /** each memTable node has a unique int value identifier, init when recovering wal */
  public static final AtomicLong memTableIdCounter = new AtomicLong(-1);

  private static final Logger logger = LoggerFactory.getLogger(AbstractMemTable.class);
  private static final int FIXED_SERIALIZED_SIZE = Byte.BYTES + 2 * Integer.BYTES + 6 * Long.BYTES;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  /** DeviceId -> chunkGroup(MeasurementId -> chunk) */
  private final Map<IDeviceID, IWritableMemChunkGroup> memTableMap;

  /**
   * The initial value is true because we want calculate the text data size when recover memTable!!
   */
  protected boolean disableMemControl = true;

  private boolean shouldFlush = false;
  private volatile FlushStatus flushStatus = FlushStatus.WORKING;
  private final int avgSeriesPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
  /** memory size of data points, including TEXT values */
  private long memSize = 0;
  /**
   * memory usage of all TVLists memory usage regardless of whether these TVLists are full,
   * including TEXT values
   */
  private long tvListRamCost = 0;

  private int seriesNumber = 0;

  private long totalPointsNum = 0;

  private long totalPointsNumThreshold = 0;

  private long maxPlanIndex = Long.MIN_VALUE;

  private long minPlanIndex = Long.MAX_VALUE;

  private final long memTableId = memTableIdCounter.incrementAndGet();

  private final long createdTime = System.currentTimeMillis();

  private static final String METRIC_POINT_IN = "pointsIn";

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
  }

  public AbstractMemTable(Map<IDeviceID, IWritableMemChunkGroup> memTableMap) {
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<IDeviceID, IWritableMemChunkGroup> getMemTableMap() {
    return memTableMap;
  }

  /**
   * create this MemChunk if it's not exist
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
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementId())) {
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
                  schemaList.stream().filter(Objects::nonNull).collect(Collectors.toList()));
            });
    for (IMeasurementSchema schema : schemaList) {
      if (schema != null && !memChunkGroup.contains(schema.getMeasurementId())) {
        seriesNumber++;
        totalPointsNumThreshold += avgSeriesPointNumThreshold;
      }
    }
    return memChunkGroup;
  }

  @Override
  public void insert(InsertRowPlan insertRowPlan) {
    // if this insert plan isn't from storage engine (mainly from test), we should set a temp device
    // id for it
    if (insertRowPlan.getDeviceID() == null) {
      insertRowPlan.setDeviceID(deviceIDFactory.getDeviceID(insertRowPlan.getDevicePath()));
    }

    updatePlanIndexes(insertRowPlan.getIndex());
    String[] measurements = insertRowPlan.getMeasurements();
    Object[] values = insertRowPlan.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    int nullPointsNumber = 0;
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // use values[i] to ignore null value
      if (values[i] == null) {
        nullPointsNumber++;
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    memSize += MemUtils.getRecordsSize(dataTypes, values, disableMemControl);
    write(insertRowPlan.getDeviceID(), schemaList, insertRowPlan.getTime(), values);

    int pointsInserted =
        insertRowPlan.getMeasurements().length
            - insertRowPlan.getFailedMeasurementNumber()
            - nullPointsNumber;

    totalPointsNum += pointsInserted;

    MetricService.getInstance()
        .count(
            pointsInserted,
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            METRIC_POINT_IN);
  }

  @Override
  public void insert(InsertRowNode insertRowNode) {
    // if this insert plan isn't from storage engine (mainly from test), we should set a temp device
    // id for it
    if (insertRowNode.getDeviceID() == null) {
      insertRowNode.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertRowNode.getDevicePath()));
    }

    // updatePlanIndexes(insertRowNode.getIndex());
    updatePlanIndexes(0);
    String[] measurements = insertRowNode.getMeasurements();
    Object[] values = insertRowNode.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    int nullPointsNumber = 0;
    for (int i = 0; i < insertRowNode.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // use values[i] to ignore null value
      if (values[i] == null) {
        nullPointsNumber++;
        continue;
      }
      IMeasurementSchema schema = insertRowNode.getMeasurementSchemas()[i];
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    memSize += MemUtils.getRecordsSize(dataTypes, values, disableMemControl);
    write(insertRowNode.getDeviceID(), schemaList, insertRowNode.getTime(), values);

    int pointsInserted =
        insertRowNode.getMeasurements().length
            - insertRowNode.getFailedMeasurementNumber()
            - nullPointsNumber;

    totalPointsNum += pointsInserted;

    MetricService.getInstance()
        .count(
            pointsInserted,
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            METRIC_POINT_IN);
  }

  @Override
  public void insertAlignedRow(InsertRowPlan insertRowPlan) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertRowPlan.getDeviceID() == null) {
      insertRowPlan.setDeviceID(deviceIDFactory.getDeviceID(insertRowPlan.getDevicePath()));
    }

    updatePlanIndexes(insertRowPlan.getIndex());
    String[] measurements = insertRowPlan.getMeasurements();
    Object[] values = insertRowPlan.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        schemaList.add(null);
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    if (schemaList.isEmpty()) {
      return;
    }
    memSize += MemUtils.getAlignedRecordsSize(dataTypes, values, disableMemControl);
    writeAlignedRow(insertRowPlan.getDeviceID(), schemaList, insertRowPlan.getTime(), values);
    int pointsInserted =
        insertRowPlan.getMeasurements().length - insertRowPlan.getFailedMeasurementNumber();
    totalPointsNum += pointsInserted;

    MetricService.getInstance()
        .count(
            pointsInserted,
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            METRIC_POINT_IN);
  }

  @Override
  public void insertAlignedRow(InsertRowNode insertRowNode) {
    // if this insert node isn't from storage engine, we should set a temp device id for it
    if (insertRowNode.getDeviceID() == null) {
      insertRowNode.setDeviceID(deviceIDFactory.getDeviceID(insertRowNode.getDevicePath()));
    }

    // TODO updatePlanIndexes(insertRowNode.getIndex());
    updatePlanIndexes(0);
    String[] measurements = insertRowNode.getMeasurements();
    Object[] values = insertRowNode.getValues();
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < insertRowNode.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        schemaList.add(null);
        continue;
      }
      IMeasurementSchema schema = insertRowNode.getMeasurementSchemas()[i];
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    if (schemaList.isEmpty()) {
      return;
    }
    memSize += MemUtils.getAlignedRecordsSize(dataTypes, values, disableMemControl);
    writeAlignedRow(insertRowNode.getDeviceID(), schemaList, insertRowNode.getTime(), values);
    int pointsInserted = insertRowNode.getMeasurements().length;
    totalPointsNum += pointsInserted;

    MetricService.getInstance()
        .count(
            pointsInserted,
            Metric.QUANTITY.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            METRIC_POINT_IN);
  }

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    updatePlanIndexes(insertTabletPlan.getIndex());
    try {
      write(insertTabletPlan, start, end);
      memSize += MemUtils.getTabletSize(insertTabletPlan, start, end, disableMemControl);
      int pointsInserted =
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum += pointsInserted;
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN);
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void insertAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    updatePlanIndexes(insertTabletPlan.getIndex());
    try {
      writeAlignedTablet(insertTabletPlan, start, end);
      memSize += MemUtils.getAlignedTabletSize(insertTabletPlan, start, end, disableMemControl);
      int pointsInserted =
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum += pointsInserted;
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN);
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void insertTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException {
    // TODO: PlanIndex
    // updatePlanIndexes(insertTabletPlan.getIndex());
    updatePlanIndexes(0);
    try {
      write(insertTabletNode, start, end);
      memSize += MemUtils.getTabletSize(insertTabletNode, start, end, disableMemControl);
      int pointsInserted = insertTabletNode.getDataTypes().length * (end - start);
      totalPointsNum += pointsInserted;
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN);
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void insertAlignedTablet(InsertTabletNode insertTabletNode, int start, int end)
      throws WriteProcessException {
    // TODO: PlanIndex
    // updatePlanIndexes(insertTabletPlan.getIndex());
    updatePlanIndexes(0);
    try {
      writeAlignedTablet(insertTabletNode, start, end);
      memSize += MemUtils.getAlignedTabletSize(insertTabletNode, start, end, disableMemControl);
      int pointsInserted = insertTabletNode.getDataTypes().length * (end - start);
      totalPointsNum += pointsInserted;
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN);
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
    memChunkGroup.write(insertTime, objectValue, schemaList);
  }

  @Override
  public void writeAlignedRow(
      IDeviceID deviceId,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.write(insertTime, objectValue, schemaList);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void write(InsertTabletPlan insertTabletPlan, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletPlan.getDeviceID() == null) {
      insertTabletPlan.setDeviceID(deviceIDFactory.getDeviceID(insertTabletPlan.getDevicePath()));
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        schemaList.add(null);
      } else {
        schemaList.add(insertTabletPlan.getMeasurementMNodes()[i].getSchema());
      }
    }
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(insertTabletPlan.getDeviceID(), schemaList);
    memChunkGroup.writeValues(
        insertTabletPlan.getTimes(),
        insertTabletPlan.getColumns(),
        insertTabletPlan.getBitMaps(),
        schemaList,
        start,
        end);
  }

  public void write(InsertTabletNode insertTabletNode, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletNode.getDeviceID() == null) {
      insertTabletNode.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertTabletNode.getDevicePath()));
    }

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
    memChunkGroup.writeValues(
        insertTabletNode.getTimes(),
        insertTabletNode.getColumns(),
        insertTabletNode.getBitMaps(),
        schemaList,
        start,
        end);
  }

  @Override
  public void writeAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletPlan.getDeviceID() == null) {
      insertTabletPlan.setDeviceID(deviceIDFactory.getDeviceID(insertTabletPlan.getDevicePath()));
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        schemaList.add(null);
      } else {
        schemaList.add(insertTabletPlan.getMeasurementMNodes()[i].getSchema());
      }
    }
    if (schemaList.isEmpty()) {
      return;
    }
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(insertTabletPlan.getDeviceID(), schemaList);
    memChunkGroup.writeValues(
        insertTabletPlan.getTimes(),
        insertTabletPlan.getColumns(),
        insertTabletPlan.getBitMaps(),
        schemaList,
        start,
        end);
  }

  public void writeAlignedTablet(InsertTabletNode insertTabletNode, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletNode.getDeviceID() == null) {
      insertTabletNode.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertTabletNode.getDevicePath()));
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletNode.getMeasurementSchemas().length; i++) {
      if (insertTabletNode.getColumns()[i] == null) {
        schemaList.add(null);
      } else {
        schemaList.add(insertTabletNode.getMeasurementSchemas()[i]);
      }
    }
    if (schemaList.isEmpty()) {
      return;
    }
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(insertTabletNode.getDeviceID(), schemaList);
    memChunkGroup.writeValues(
        insertTabletNode.getTimes(),
        insertTabletNode.getColumns(),
        insertTabletNode.getBitMaps(),
        schemaList,
        start,
        end);
  }

  @Override
  public boolean checkIfChunkDoesNotExist(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return true;
    }
    return !memChunkGroup.contains(measurement);
  }

  @Override
  public long getCurrentTVListSize(IDeviceID deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
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
  public boolean reachTotalPointNumThreshold() {
    if (totalPointsNum == 0) {
      return false;
    }
    return totalPointsNum >= totalPointsNumThreshold;
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
      PartialPath fullPath, long ttlLowerBound, List<Pair<Modification, IMemTable>> modsToMemtable)
      throws IOException, QueryProcessException {
    return ResourceByPathUtils.getResourceInstance(fullPath)
        .getReadOnlyMemChunkFromMemTable(this, modsToMemtable, ttlLowerBound);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(getDeviceID(devicePath));
    if (memChunkGroup == null) {
      return;
    }
    totalPointsNum -= memChunkGroup.delete(originalPath, devicePath, startTimestamp, endTimestamp);
    if (memChunkGroup.getMemChunkMap().isEmpty()) {
      memTableMap.remove(getDeviceID(devicePath));
    }
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

  void updatePlanIndexes(long index) {
    maxPlanIndex = Math.max(index, maxPlanIndex);
    minPlanIndex = Math.min(index, minPlanIndex);
  }

  @Override
  public long getMemTableId() {
    return memTableId;
  }

  @Override
  public long getCreatedTime() {
    return createdTime;
  }

  @Override
  public FlushStatus getFlushStatus() {
    return flushStatus;
  }

  @Override
  public void setFlushStatus(FlushStatus flushStatus) {
    this.flushStatus = flushStatus;
  }

  private IDeviceID getDeviceID(PartialPath deviceId) {
    return deviceIDFactory.getDeviceID(deviceId);
  }

  /** Notice: this method is concurrent unsafe */
  @Override
  public int serializedSize() {
    if (isSignalMemTable()) {
      return Byte.BYTES;
    }
    int size = FIXED_SERIALIZED_SIZE;
    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      size += ReadWriteIOUtils.sizeToWrite(entry.getKey().toStringID());
      size += Byte.BYTES;
      size += entry.getValue().serializedSize();
    }
    return size;
  }

  /** Notice: this method is concurrent unsafe */
  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
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
      WALWriteUtils.write(entry.getKey().toStringID(), buffer);

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
      IDeviceID deviceID = deviceIDFactory.getDeviceID(ReadWriteIOUtils.readString(stream));

      boolean isAligned = ReadWriteIOUtils.readBool(stream);
      IWritableMemChunkGroup memChunkGroup;
      if (isAligned) {
        memChunkGroup = AlignedWritableMemChunkGroup.deserialize(stream);
      } else {
        memChunkGroup = WritableMemChunkGroup.deserialize(stream);
      }
      memTableMap.put(deviceID, memChunkGroup);
    }
  }

  public static class Factory {
    private Factory() {}

    public static IMemTable create(DataInputStream stream) throws IOException {
      boolean isSignal = ReadWriteIOUtils.readBool(stream);
      IMemTable memTable;
      if (isSignal) {
        memTable = new NotifyFlushMemTable();
      } else {
        PrimitiveMemTable primitiveMemTable = new PrimitiveMemTable();
        primitiveMemTable.deserialize(stream);
        memTable = primitiveMemTable;
      }
      return memTable;
    }
  }
}
