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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMemTable implements IMemTable {

  /** DeviceId -> chunkGroup(MeasurementId -> chunk) */
  private final ConcurrentMap<IDeviceID, IWritableMemChunkGroup> memTableMap;

  /**
   * The initial value is true because we want calculate the text data size when recover memTable!!
   */
  protected volatile boolean disableMemControl = true;

  private static final Logger logger = LoggerFactory.getLogger(AbstractMemTable.class);

  private volatile boolean shouldFlush = false;
  private final int avgSeriesPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
  /** memory size of data points, including TEXT values */
  private AtomicLong memSize = new AtomicLong();
  /**
   * memory usage of all TVLists memory usage regardless of whether these TVLists are full,
   * including TEXT values
   */
  private AtomicLong tvListRamCost = new AtomicLong();

  private AtomicInteger seriesNumber = new AtomicInteger();

  private AtomicLong totalPointsNum = new AtomicLong();

  private AtomicLong totalPointsNumThreshold = new AtomicLong();

  private volatile long maxPlanIndex = Long.MIN_VALUE;

  private volatile long minPlanIndex = Long.MAX_VALUE;

  private final long createdTime = System.currentTimeMillis();

  private static final String METRIC_POINT_IN = "pointsIn";

  public AbstractMemTable() {
    this.memTableMap = new ConcurrentHashMap<>();
  }

  public AbstractMemTable(ConcurrentMap<IDeviceID, IWritableMemChunkGroup> memTableMap) {
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
      if (!memChunkGroup.contains(schema.getMeasurementId())) {
        seriesNumber.getAndIncrement();
        totalPointsNumThreshold.getAndAdd(avgSeriesPointNumThreshold);
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
              seriesNumber.getAndAdd(schemaList.size());
              totalPointsNumThreshold.getAndAdd(
                  ((long) avgSeriesPointNumThreshold) * schemaList.size());
              return new AlignedWritableMemChunkGroup(schemaList);
            });
    for (IMeasurementSchema schema : schemaList) {
      if (!memChunkGroup.contains(schema.getMeasurementId())) {
        seriesNumber.getAndIncrement();
        totalPointsNumThreshold.getAndAdd(avgSeriesPointNumThreshold);
      }
    }
    return memChunkGroup;
  }

  @Override
  public void insert(InsertRowPlan insertRowPlan) {
    // if this insert plan isn't from storage engine (mainly from test), we should set a temp device
    // id for it
    if (insertRowPlan.getDeviceID() == null) {
      insertRowPlan.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertRowPlan.getDevicePath()));
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
    memSize.getAndAdd(MemUtils.getRecordsSize(dataTypes, values, disableMemControl));
    write(
        insertRowPlan.getDeviceID(),
        insertRowPlan.getFailedIndices(),
        schemaList,
        insertRowPlan.getTime(),
        values);

    int pointsInserted =
        insertRowPlan.getMeasurements().length
            - insertRowPlan.getFailedMeasurementNumber()
            - nullPointsNumber;

    totalPointsNum.getAndAdd(pointsInserted);

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN,
              Tag.TYPE.toString(),
              "insertRow");
    }
  }

  @Override
  public void insertAlignedRow(InsertRowPlan insertRowPlan) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertRowPlan.getDeviceID() == null) {
      insertRowPlan.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertRowPlan.getDevicePath()));
    }

    updatePlanIndexes(insertRowPlan.getIndex());
    String[] measurements = insertRowPlan.getMeasurements();
    Object[] values = insertRowPlan.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      // use measurements[i] to ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
      dataTypes.add(schema.getType());
    }
    if (schemaList.isEmpty()) {
      return;
    }
    memSize.getAndAdd(MemUtils.getAlignedRecordsSize(dataTypes, values, disableMemControl));
    writeAlignedRow(
        insertRowPlan.getDeviceID(),
        insertRowPlan.getFailedIndices(),
        schemaList,
        insertRowPlan.getTime(),
        values);
    int pointsInserted =
        insertRowPlan.getMeasurements().length - insertRowPlan.getFailedMeasurementNumber();
    totalPointsNum.getAndAdd(pointsInserted);

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricService.getInstance()
          .count(
              pointsInserted,
              Metric.QUANTITY.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              METRIC_POINT_IN,
              Tag.TYPE.toString(),
              "insertAlignedRow");
    }
  }

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    updatePlanIndexes(insertTabletPlan.getIndex());
    try {
      write(insertTabletPlan, start, end);
      memSize.getAndAdd(MemUtils.getTabletSize(insertTabletPlan, start, end, disableMemControl));
      int pointsInserted =
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum.getAndAdd(pointsInserted);
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
        MetricService.getInstance()
            .count(
                pointsInserted,
                Metric.QUANTITY.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                METRIC_POINT_IN,
                Tag.TYPE.toString(),
                "insertTablet");
      }
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
      memSize.getAndAdd(
          MemUtils.getAlignedTabletSize(insertTabletPlan, start, end, disableMemControl));
      int pointsInserted =
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
      totalPointsNum.getAndAdd(pointsInserted);
      if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
        MetricService.getInstance()
            .count(
                pointsInserted,
                Metric.QUANTITY.toString(),
                MetricLevel.IMPORTANT,
                Tag.NAME.toString(),
                METRIC_POINT_IN,
                Tag.TYPE.toString(),
                "insertAlignedTablet");
      }
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void write(
      IDeviceID deviceId,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.write(insertTime, objectValue, failedIndices, schemaList);
  }

  @Override
  public void writeAlignedRow(
      IDeviceID deviceId,
      List<Integer> failedIndices,
      List<IMeasurementSchema> schemaList,
      long insertTime,
      Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.write(insertTime, objectValue, failedIndices, schemaList);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void write(InsertTabletPlan insertTabletPlan, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletPlan.getDeviceID() == null) {
      insertTabletPlan.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertTabletPlan.getDevicePath()));
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertTabletPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
    }
    IWritableMemChunkGroup memChunkGroup =
        createMemChunkGroupIfNotExistAndGet(insertTabletPlan.getDeviceID(), schemaList);
    memChunkGroup.writeValues(
        insertTabletPlan.getTimes(),
        insertTabletPlan.getColumns(),
        insertTabletPlan.getBitMaps(),
        insertTabletPlan.getFailedIndices(),
        schemaList,
        start,
        end);
  }

  @Override
  public void writeAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end) {
    // if this insert plan isn't from storage engine, we should set a temp device id for it
    if (insertTabletPlan.getDeviceID() == null) {
      insertTabletPlan.setDeviceID(
          DeviceIDFactory.getInstance().getDeviceID(insertTabletPlan.getDevicePath()));
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertTabletPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
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
        insertTabletPlan.getFailedIndices(),
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
    return seriesNumber.get();
  }

  @Override
  public long getTotalPointsNum() {
    return totalPointsNum.get();
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
    return memSize.get();
  }

  @Override
  public boolean reachTotalPointNumThreshold() {
    if (totalPointsNum.get() == 0) {
      return false;
    }
    return totalPointsNum.get() >= totalPointsNumThreshold.get();
  }

  @Override
  public void clear() {
    memTableMap.clear();
    memSize = new AtomicLong();
    seriesNumber = new AtomicInteger();
    totalPointsNum = new AtomicLong();
    totalPointsNumThreshold = new AtomicLong();
    tvListRamCost = new AtomicLong();
    maxPlanIndex = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk query(
      PartialPath fullPath, long ttlLowerBound, List<Pair<Modification, IMemTable>> modsToMemtable)
      throws IOException, QueryProcessException {
    return fullPath.getReadOnlyMemChunkFromMemTable(this, modsToMemtable, ttlLowerBound);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(getDeviceID(devicePath));
    if (memChunkGroup == null) {
      return;
    }
    totalPointsNum.getAndAdd(
        -memChunkGroup.delete(originalPath, devicePath, startTimestamp, endTimestamp));
    if (memChunkGroup.getMemChunkMap().isEmpty()) {
      memTableMap.remove(getDeviceID(devicePath));
    }
  }

  @Override
  public void addTVListRamCost(long cost) {
    this.tvListRamCost.getAndAdd(cost);
  }

  @Override
  public void releaseTVListRamCost(long cost) {
    this.tvListRamCost.getAndAdd(-cost);
  }

  @Override
  public long getTVListsRamCost() {
    return tvListRamCost.get();
  }

  @Override
  public void addTextDataSize(long textDataSize) {
    this.memSize.getAndAdd(textDataSize);
  }

  @Override
  public void releaseTextDataSize(long textDataSize) {
    this.memSize.getAndAdd(-textDataSize);
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
  public long getCreatedTime() {
    return createdTime;
  }

  private IDeviceID getDeviceID(PartialPath deviceId) {
    return DeviceIDFactory.getInstance().getDeviceID(deviceId);
  }
}
