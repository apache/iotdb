/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.overflow.io;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

/**
 * This class is used to store and query all overflow data in memory.<br>
 * This just represent someone storage group.<br>
 */
public class OverflowSupport {

  /**
   * store update and delete data
   */
  private Map<String, Map<String, OverflowSeriesImpl>> indexTrees;

  /**
   * store insert data
   */
  private IMemTable memTable;

  public OverflowSupport() {
    indexTrees = new HashMap<>();
    memTable = new PrimitiveMemTable();
  }

  public void insert(TSRecord tsRecord) {
    for (DataPoint dataPoint : tsRecord.dataPointList) {
      memTable.write(tsRecord.deviceId, dataPoint.getMeasurementId(), dataPoint.getType(),
              tsRecord.time,
              dataPoint.getValue().toString());
    }
  }

  /**
   * @deprecated update time series data
   */
  @Deprecated
  public void update(String deviceId, String measurementId, long startTime, long endTime,
                     TSDataType dataType,
                     byte[] value) {
    if (!indexTrees.containsKey(deviceId)) {
      indexTrees.put(deviceId, new HashMap<>());
    }
    if (!indexTrees.get(deviceId).containsKey(measurementId)) {
      indexTrees.get(deviceId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
    }
    indexTrees.get(deviceId).get(measurementId).update(startTime, endTime);
  }

  public void delete(String deviceId, String measurementId, long timestamp, boolean isFlushing) {
    if (isFlushing) {
      memTable = memTable.copy();
      memTable.delete(deviceId, measurementId, timestamp);
    } else {
      memTable.delete(deviceId, measurementId, timestamp);
    }
  }

  public ReadOnlyMemChunk queryOverflowInsertInMemory(String deviceId, String measurementId,
      TSDataType dataType, Map<String, String> props) {
    return memTable.query(deviceId, measurementId, dataType, props);
  }

  public BatchData queryOverflowUpdateInMemory(String deviceId, String measurementId,
      TSDataType dataType) {
    if (indexTrees.containsKey(deviceId) && indexTrees.get(deviceId).containsKey(measurementId)
        && indexTrees.get(deviceId).get(measurementId).getDataType().equals(dataType)) {
      return indexTrees.get(deviceId).get(measurementId).query();
    }
    return null;
  }

  public boolean isEmptyOfOverflowSeriesMap() {
    return indexTrees.isEmpty();
  }

  public Map<String, Map<String, OverflowSeriesImpl>> getOverflowSeriesMap() {
    return indexTrees;
  }

  public boolean isEmptyOfMemTable() {
    return memTable.isEmpty();
  }

  public IMemTable getMemTabale() {
    return memTable;
  }

  public long getSize() {
    // TODO: calculate the size of this overflow support
    return 0;
  }

  public void clear() {
    indexTrees.clear();
    memTable.clear();
  }
}
