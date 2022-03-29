/*
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
package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.commons.partition.TimePartitionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertTabletStatement extends InsertBaseStatement {

  private long[] times; // times should be sorted. It is done in the session API.
  private BitMap[] bitMaps;
  private Object[] columns;

  private int rowCount = 0;

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  @Override
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    super.markFailedMeasurementInsertion(index, e);
    columns[index] = null;
  }

  @Override
  public List<TimePartitionId> getTimePartitionIds() {
    List<TimePartitionId> result = new ArrayList<>();
    long startTime =
        (times[0] / StorageEngine.getTimePartitionInterval())
            * StorageEngine.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngine.getTimePartitionInterval(); // excluded
    TimePartitionId timePartitionId = StorageEngine.getTimePartitionId(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        result.add(timePartitionId);
        // next init
        endTime =
            (times[i] / StorageEngine.getTimePartitionInterval() + 1)
                * StorageEngine.getTimePartitionInterval();
        timePartitionId = StorageEngine.getTimePartitionId(times[i]);
      }
    }
    result.add(timePartitionId);
    return result;
  }

  @Override
  public boolean checkDataType(SchemaTree schemaTree) {
    List<MeasurementPath> measurementPaths =
        schemaTree.searchMeasurementPaths(devicePath, Arrays.asList(measurements));
    for (int i = 0; i < measurementPaths.size(); i++) {
      if (dataTypes[i] != measurementPaths.get(i).getSeriesType()) {
        if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          return false;
        } else {
          markFailedMeasurementInsertion(
              i,
              new DataTypeMismatchException(
                  measurements[i], measurementPaths.get(i).getSeriesType(), dataTypes[i]));
        }
      }
    }
    return true;
  }
}
