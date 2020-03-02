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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class AbstractMemTable implements IMemTable {

  private long version = Long.MAX_VALUE;

  private List<Modification> modifications = new ArrayList<>();

  private final Map<String, Map<String, IWritableMemChunk>> memTableMap;

  private long memSize = 0;

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
  }

  public AbstractMemTable(Map<String, Map<String, IWritableMemChunk>> memTableMap) {
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<String, Map<String, IWritableMemChunk>> getMemTableMap() {
    return memTableMap;
  }

  /**
   * check whether the given seriesPath is within this memtable.
   *
   * @return true if seriesPath is within this memtable
   */
  private boolean checkPath(String deviceId, String measurement) {
    return memTableMap.containsKey(deviceId) && memTableMap.get(deviceId).containsKey(measurement);
  }

  private IWritableMemChunk createIfNotExistAndGet(String deviceId, String measurement,
      TSDataType dataType) {
    if (!memTableMap.containsKey(deviceId)) {
      memTableMap.put(deviceId, new HashMap<>());
    }
    Map<String, IWritableMemChunk> memSeries = memTableMap.get(deviceId);
    if (!memSeries.containsKey(measurement)) {
      memSeries.put(measurement, genMemSeries(dataType));
    }
    return memSeries.get(measurement);
  }

  protected abstract IWritableMemChunk genMemSeries(TSDataType dataType);

  @Override
  public void insert(InsertPlan insertPlan) throws QueryProcessException {
    try {
      for (int i = 0; i < insertPlan.getValues().length; i++) {

        Object value = CommonUtils.parseValue(insertPlan.getDataTypes()[i], insertPlan.getValues()[i]);
        write(insertPlan.getDeviceId(), insertPlan.getMeasurements()[i],
            insertPlan.getDataTypes()[i], insertPlan.getTime(), value);
      }
      long recordSizeInByte = MemUtils.getRecordSize(insertPlan);
      memSize += recordSizeInByte;
    } catch (RuntimeException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public void insertBatch(BatchInsertPlan batchInsertPlan, int start, int end)
      throws QueryProcessException {
    try {
      write(batchInsertPlan, start, end);
      long recordSizeInByte = MemUtils.getRecordSize(batchInsertPlan, start, end);
      memSize += recordSizeInByte;
    } catch (RuntimeException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }


  @Override
  public void write(String deviceId, String measurement, TSDataType dataType, long insertTime,
      Object objectValue) {
    IWritableMemChunk memSeries = createIfNotExistAndGet(deviceId, measurement, dataType);
    memSeries.write(insertTime, objectValue);
  }

  @Override
  public void write(BatchInsertPlan batchInsertPlan, int start, int end) {
    for (int i = 0; i < batchInsertPlan.getMeasurements().length; i++) {
      IWritableMemChunk memSeries = createIfNotExistAndGet(batchInsertPlan.getDeviceId(),
          batchInsertPlan.getMeasurements()[i], batchInsertPlan.getDataTypes()[i]);
      memSeries.write(batchInsertPlan.getTimes(), batchInsertPlan.getColumns()[i],
          batchInsertPlan.getDataTypes()[i], start, end);
    }
  }


  @Override
  public long size() {
    long sum = 0;
    for (Map<String, IWritableMemChunk> seriesMap : memTableMap.values()) {
      for (IWritableMemChunk writableMemChunk : seriesMap.values()) {
        sum += writableMemChunk.count();
      }
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
    modifications.clear();
    memSize = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk query(String deviceId, String measurement, TSDataType dataType,
      TSEncoding encoding, Map<String, String> props, long timeLowerBound)
      throws IOException, QueryProcessException {
    if (!checkPath(deviceId, measurement)) {
      return null;
    }
    long undeletedTime = findUndeletedTime(deviceId, measurement, timeLowerBound);
    IWritableMemChunk memChunk = memTableMap.get(deviceId).get(measurement);
    TVList chunkCopy = memChunk.getTVList().clone();

    chunkCopy.setTimeOffset(undeletedTime);
    return new ReadOnlyMemChunk(measurement, dataType, encoding, chunkCopy, props, getVersion());
  }


  private long findUndeletedTime(String deviceId, String measurement, long timeLowerBound) {
    long undeletedTime = Long.MIN_VALUE;
    for (Modification modification : modifications) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getDevice().equals(deviceId) && deletion.getMeasurement().equals(measurement)
            && deletion.getTimestamp() > undeletedTime) {
          undeletedTime = deletion.getTimestamp();
        }
      }
    }
    return Math.max(undeletedTime + 1, timeLowerBound);
  }

  @Override
  public void delete(String deviceId, String measurementId, long timestamp) {
    Map<String, IWritableMemChunk> deviceMap = memTableMap.get(deviceId);
    if (deviceMap != null) {
      IWritableMemChunk chunk = deviceMap.get(measurementId);
      if (chunk == null) {
        return;
      }
      chunk.delete(timestamp);
    }
  }

  @Override
  public void delete(Deletion deletion) {
    this.modifications.add(deletion);
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public void release() {
    for (Entry<String, Map<String, IWritableMemChunk>> entry : memTableMap.entrySet()) {
      for (Entry<String, IWritableMemChunk> subEntry : entry.getValue().entrySet()) {
        TVListAllocator.getInstance().release(subEntry.getValue().getTVList());
      }
    }
  }
}
