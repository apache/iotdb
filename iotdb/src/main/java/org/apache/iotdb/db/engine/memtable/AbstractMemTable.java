/**
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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.monitor.collector.MemTableWriteTimeCost;
import org.apache.iotdb.db.monitor.collector.MemTableWriteTimeCost.MemTableWriteTimeCostType;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class AbstractMemTable implements IMemTable {

  private long version;

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
  public void insert(InsertPlan insertPlan) {
    for (int i = 0; i < insertPlan.getValues().length; i++) {
      write(insertPlan.getDeviceId(), insertPlan.getMeasurements()[i],
          insertPlan.getDataTypes()[i], insertPlan.getTime(), insertPlan.getValues()[i]);

    }
    long recordSizeInByte = MemUtils.getRecordSize(insertPlan);
    memSize += recordSizeInByte;
  }

  @Override
  public void write(String deviceId, String measurement, TSDataType dataType, long insertTime,
      String insertValue) {
    IWritableMemChunk memSeries = createIfNotExistAndGet(deviceId, measurement, dataType);
    memSeries.write(insertTime, insertValue);
  }

  @Override
  public void write(String deviceId, String measurement, TSDataType dataType, long insertTime,
      Object value) {
    IWritableMemChunk memSeries = createIfNotExistAndGet(deviceId, measurement, dataType);
    memSeries.write(insertTime, value);
    // update memory size of current memtable
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
//    for (Map<String, IWritableMemChunk> writableMemChunkMap : memTableMap.values()) {
//      for (IWritableMemChunk memChunk : writableMemChunkMap.values()) {
//        memChunk.releasePrimitiveArrayList();
//      }
//    }
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
      Map<String, String> props) {

    return new ReadOnlyMemChunk(dataType, getSeriesData(deviceId,
        measurement, dataType), props);
  }

  private long findUndeletedTime(String deviceId, String measurement) {
    String path = deviceId + PATH_SEPARATOR + measurement;
    long undeletedTime = 0;
    for (Modification modification : modifications) {
      if (modification instanceof  Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPathString().equals(path) && deletion.getTimestamp() > undeletedTime) {
          undeletedTime = deletion.getTimestamp();
        }
      }
    }
    return undeletedTime + 1;
  }

  private TimeValuePairSorter getSeriesData(String deviceId, String measurement, TSDataType dataType) {
    if (!checkPath(deviceId, measurement)) {
      return new WritableMemChunk(dataType);
    }
    long undeletedTime = findUndeletedTime(deviceId, measurement);
    IWritableMemChunk memChunk = memTableMap.get(deviceId).get(measurement);
    memChunk.setTimeOffset(undeletedTime);
    return memChunk;
  }

  @Override
  public boolean delete(String deviceId, String measurementId, long timestamp) {
    Map<String, IWritableMemChunk> deviceMap = memTableMap.get(deviceId);
    if (deviceMap != null) {
      IWritableMemChunk chunk = deviceMap.get(measurementId);
      if (chunk == null) {
        return true;
      }
      IWritableMemChunk newChunk = filterChunk(chunk, timestamp);
      if (newChunk != null) {
        deviceMap.put(measurementId, newChunk);
        return newChunk.count() != chunk.count();
      }
    }
    return false;
  }

  @Override
  public boolean delete(Deletion deletion) {
    return this.modifications.add(deletion);
  }

  /**
   * If chunk contains data with timestamp less than 'timestamp', create a copy and delete all those
   * data. Otherwise return null.
   *
   * @param chunk the source chunk.
   * @param timestamp the upper-bound of deletion time.
   * @return A reduced copy of chunk if chunk contains data with timestamp less than 'timestamp', of
   * null.
   */
  private IWritableMemChunk filterChunk(IWritableMemChunk chunk, long timestamp) {
    List<TimeValuePair> timeValuePairs = chunk.getSortedTimeValuePairList();
    if (!timeValuePairs.isEmpty() && timeValuePairs.get(0).getTimestamp() <= timestamp) {
      TSDataType dataType = chunk.getType();
      IWritableMemChunk newChunk = genMemSeries(dataType);
      for (TimeValuePair pair : timeValuePairs) {
        if (pair.getTimestamp() > timestamp) {
          switch (dataType) {
            case BOOLEAN:
              newChunk.putBoolean(pair.getTimestamp(), pair.getValue().getBoolean());
              break;
            case DOUBLE:
              newChunk.putDouble(pair.getTimestamp(), pair.getValue().getDouble());
              break;
            case INT64:
              newChunk.putLong(pair.getTimestamp(), pair.getValue().getLong());
              break;
            case INT32:
              newChunk.putInt(pair.getTimestamp(), pair.getValue().getInt());
              break;
            case FLOAT:
              newChunk.putFloat(pair.getTimestamp(), pair.getValue().getFloat());
              break;
            case TEXT:
              newChunk.putBinary(pair.getTimestamp(), pair.getValue().getBinary());
              break;
            default:
                throw new UnsupportedOperationException("Unknown datatype: " + dataType);
          }
        }
      }
      return newChunk;
    }
    return null;
  }

  @Override
  public boolean containSeries(String deviceId, String measurementId) {
    Map<String, IWritableMemChunk> deviceMap = memTableMap.get(deviceId);
    return deviceMap != null && deviceMap.containsKey(measurementId);
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getVersion() {
    return version;
  }
}
