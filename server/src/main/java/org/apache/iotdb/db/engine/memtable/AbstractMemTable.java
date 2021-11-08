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
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractMemTable implements IMemTable {

  private final Map<String, IWritableMemChunkGroup> memTableMap;
  /**
   * The initial value is true because we want calculate the text data size when recover memTable!!
   */
  protected boolean disableMemControl = true;

  private boolean shouldFlush = false;
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

  private long createdTime = System.currentTimeMillis();

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
  }

  public AbstractMemTable(Map<String, IWritableMemChunkGroup> memTableMap) {
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<String, IWritableMemChunkGroup> getMemTableMap() {
    return memTableMap;
  }

  /**
   * create this MemChunk if it's not exist
   *
   * @param deviceId device id
   * @param schema measurement schema
   * @return this MemChunk
   */
  private IWritableMemChunkGroup createMemChunkGroupIfNotExistAndGet(
      String deviceId, List<IMeasurementSchema> schemaList) {
    IWritableMemChunkGroup memChunkGroup =
        memTableMap.putIfAbsent(deviceId, new WritableMemChunkGroup(schemaList));

    return memSeries.computeIfAbsent(
        schema.getMeasurementId(),
        k -> {
          seriesNumber++;
          totalPointsNumThreshold += avgSeriesPointNumThreshold;
          return genMemChunkGroup(schema);
        });
  }

  private IWritableMemChunkGroup createAlignedMemChunkGroupIfNotExistAndGet(
      String deviceId, List<IMeasurementSchema> schemaList) {
    IWritableMemChunkGroup memChunkGroup =
        memTableMap.putIfAbsent(deviceId, new AlignedWritableMemChunkGroup(schemaList));

    return memChunkGroup.computeIfAbsent(
        vectorSchema.getMeasurementId(),
        k -> {
          seriesNumber++;
          totalPointsNumThreshold +=
              avgSeriesPointNumThreshold * vectorSchema.getSubMeasurementsCount();
          return genAlignedMemChunkGroup(vectorSchema);
        });
  }

  protected abstract IWritableMemChunkGroup genMemChunkGroup(List<IMeasurementSchema> schemaList);

  protected abstract IWritableMemChunkGroup genAlignedMemChunkGroup(List<IMeasurementSchema> schemaList);

  @Override
  public void insert(InsertRowPlan insertRowPlan) {
    updatePlanIndexes(insertRowPlan.getIndex());
    Object[] values = insertRowPlan.getValues();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      if (insertRowPlan.getMeasurements()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
    }
    memSize +=
        MemUtils.getRecordSize(
            schemaList, values, disableMemControl);
    write(
        insertRowPlan.getPrefixPath().getFullPath(),
        schemaList,
        insertRowPlan.getTime(),
        values);
    totalPointsNum +=
        insertRowPlan.getMeasurements().length - insertRowPlan.getFailedMeasurementNumber();
  }

  @Override
  public void insertAlignedRow(InsertRowPlan insertRowPlan) {
    updatePlanIndexes(insertRowPlan.getIndex());
    // write vector
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertRowPlan.getMeasurements().length; i++) {
      if (insertRowPlan.getMeasurements()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
    }
    memSize += MemUtils.getAlignedRecordSize(schemaList, insertRowPlan.getValues(), disableMemControl);
    writeAlignedRow(
        insertRowPlan.getPrefixPath().getFullPath(),
        schemaList,
        insertRowPlan.getTime(),
        insertRowPlan.getValues());
    totalPointsNum +=
        insertRowPlan.getMeasurements().length - insertRowPlan.getFailedMeasurementNumber();
  }

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    updatePlanIndexes(insertTabletPlan.getIndex());
    try {
      write(insertTabletPlan, start, end);
      memSize += MemUtils.getRecordSize(insertTabletPlan, start, end, disableMemControl);
      totalPointsNum +=
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
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
      memSize += MemUtils.getAlignedRecordSize(insertTabletPlan, start, end, disableMemControl);
      totalPointsNum +=
          (insertTabletPlan.getDataTypes().length - insertTabletPlan.getFailedMeasurementNumber())
              * (end - start);
    } catch (RuntimeException e) {
      throw new WriteProcessException(e);
    }
  }

  @Override
  public void write(
      String deviceId, List<IMeasurementSchema> schemaList, long insertTime, Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup = createMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.write(insertTime, objectValue);
  }

  @Override
  public void writeAlignedRow(
      String deviceId, List<IMeasurementSchema> schemaList, long insertTime, Object[] objectValue) {
    IWritableMemChunkGroup memChunkGroup = createAlignedMemChunkGroupIfNotExistAndGet(deviceId, schemaList);
    memChunkGroup.write(insertTime, objectValue);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void write(InsertTabletPlan insertTabletPlan, int start, int end) {
    updatePlanIndexes(insertTabletPlan.getIndex());
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertTabletPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
    }
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(
            insertTabletPlan.getPrefixPath().getFullPath(), schemaList);
    memChunkGroup.writeValues(
        insertTabletPlan.getTimes(),
        insertTabletPlan.getColumns(),
        insertTabletPlan.getBitMaps(),
        schemaList,
        start,
        end);
  }

  public void writeAlignedTablet(InsertTabletPlan insertTabletPlan, int start, int end) {
    updatePlanIndexes(insertTabletPlan.getIndex());
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < insertTabletPlan.getMeasurements().length; i++) {
      if (insertTabletPlan.getColumns()[i] == null) {
        continue;
      }
      IMeasurementSchema schema = insertTabletPlan.getMeasurementMNodes()[i].getSchema();
      schemaList.add(schema);
    }
    IWritableMemChunkGroup memChunkGroup =
        createAlignedMemChunkGroupIfNotExistAndGet(
            insertTabletPlan.getPrefixPath().getFullPath(), schemaList);
    memChunkGroup.writeValues(
        insertTabletPlan.getTimes(),
        insertTabletPlan.getColumns(),
        insertTabletPlan.getBitMaps(),
        schemaList,
        start,
        end);
  }

  @Override
  public boolean checkIfChunkDoesNotExist(String deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    if (null == memChunkGroup) {
      return true;
    }
    return !memChunkGroup.contains(measurement);
  }

  @Override
  public long getCurrentChunkPointNum(String deviceId, String measurement) {
    IWritableMemChunkGroup memChunkGroup = memTableMap.get(deviceId);
    return memChunkGroup.count();
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
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk query(
      PartialPath fullPath, long ttlLowerBound, List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    return fullPath.getReadOnlyMemChunkFromMemTable(memTableMap, deletionList);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  @Override
  public void delete(
      PartialPath originalPath, PartialPath devicePath, long startTimestamp, long endTimestamp) {
    Map<String, IWritableMemChunk> deviceMap = memTableMap.get(devicePath.getFullPath());
    if (deviceMap == null) {
      return;
    }

    Iterator<Entry<String, IWritableMemChunk>> iter = deviceMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, IWritableMemChunk> entry = iter.next();
      IWritableMemChunk chunk = entry.getValue();
      // the key is measurement rather than component of multiMeasurement
      PartialPath fullPath = devicePath.concatNode(entry.getKey());
      if (originalPath.matchFullPath(fullPath)) {
        // matchFullPath ensures this branch could work on delete data of unary or multi measurement
        // and delete timeseries or aligned timeseries
        if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
          iter.remove();
        }
        int deletedPointsNumber = chunk.delete(startTimestamp, endTimestamp);
        totalPointsNum -= deletedPointsNumber;
      }
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
    for (Entry<String, IWritableMemChunkGroup> entry : memTableMap.entrySet()) {
      entry.getValue().release();
//      for (Entry<String, IWritableMemChunk> subEntry : entry.getValue().entrySet()) {
//        TVList list = subEntry.getValue().getTVList();
//        if (list.getReferenceCount() == 0) {
//          TVListAllocator.getInstance().release(list);
//        }
//      }
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
}
