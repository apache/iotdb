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

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class AbstractMemTable implements IMemTable {

  private final Map<String, Map<String, IWritableMemChunk>> memTableMap;

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
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
  public void write(String deviceId, String measurement, TSDataType dataType, long insertTime,
      String insertValue) {
    IWritableMemChunk memSeries = createIfNotExistAndGet(deviceId, measurement, dataType);
    memSeries.write(insertTime, insertValue);
  }

  @Override
  public int size() {
    int sum = 0;
    for (Map<String, IWritableMemChunk> seriesMap : memTableMap.values()) {
      for (IWritableMemChunk writableMemChunk : seriesMap.values()) {
        sum += writableMemChunk.count();
      }
    }
    return sum;
  }

  @Override
  public void clear() {
    memTableMap.clear();
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public TimeValuePairSorter query(String deviceId, String measurement, TSDataType dataType) {
    if (!checkPath(deviceId, measurement)) {
      return new WritableMemChunk(dataType);
    }
    return memTableMap.get(deviceId).get(measurement);
  }

}
