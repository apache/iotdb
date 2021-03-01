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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RawQueryDataSetWithValueFilter extends QueryDataSet implements UDFInputDataSet {

  private final TimeGenerator timeGenerator;
  private final List<IReaderByTimestamp> seriesReaderByTimestampList;
  private final List<Boolean> cached;

  private boolean hasCachedRow;
  private RowRecord cachedRowRecord;
  private Object[] cachedRowInObjects;

  /**
   * constructor of EngineDataSetWithValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param timeGenerator EngineTimeGenerator object
   * @param readers readers in List(IReaderByTimeStamp) structure
   * @param ascending specifies how the data should be sorted,'True' means read in ascending time
   *     order, and 'false' means read in descending time order
   */
  public RawQueryDataSetWithValueFilter(
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<IReaderByTimestamp> readers,
      List<Boolean> cached,
      boolean ascending) {
    super(new ArrayList<>(paths), dataTypes, ascending);
    this.timeGenerator = timeGenerator;
    this.seriesReaderByTimestampList = readers;
    this.cached = cached;
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    if (hasCachedRow) {
      return true;
    }
    return cacheRowRecord();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedRow && !cacheRowRecord()) {
      return null;
    }
    hasCachedRow = false;
    return cachedRowRecord;
  }

  /**
   * Cache row record
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecord() throws IOException {
    while (timeGenerator.hasNext()) {
      boolean hasField = false;
      long timestamp = timeGenerator.next();
      RowRecord rowRecord = new RowRecord(timestamp);

      for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
        Object value;
        // get value from readers in time generator
        if (cached.get(i)) {
          value = timeGenerator.getValue(paths.get(i), timestamp);
        } else {
          // get value from series reader without filter
          IReaderByTimestamp reader = seriesReaderByTimestampList.get(i);
          value = reader.getValueInTimestamp(timestamp);
        }
        if (value == null) {
          rowRecord.addField(null);
        } else {
          hasField = true;
          rowRecord.addField(value, dataTypes.get(i));
        }
      }
      if (hasField) {
        hasCachedRow = true;
        cachedRowRecord = rowRecord;
        break;
      }
    }
    return hasCachedRow;
  }

  @Override
  public boolean hasNextRowInObjects() throws IOException {
    if (hasCachedRow) {
      return true;
    }
    return cacheRowInObjects();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    if (!hasCachedRow && !cacheRowInObjects()) {
      // values + timestamp
      return new Object[seriesReaderByTimestampList.size() + 1];
    }
    hasCachedRow = false;
    return cachedRowInObjects;
  }

  private boolean cacheRowInObjects() throws IOException {
    int seriesNumber = seriesReaderByTimestampList.size();
    while (timeGenerator.hasNext()) {
      boolean hasField = false;

      Object[] rowInObjects = new Object[seriesNumber + 1];
      long timestamp = timeGenerator.next();
      rowInObjects[seriesNumber] = timestamp;

      for (int i = 0; i < seriesNumber; i++) {
        Object value =
            cached.get(i)
                ? timeGenerator.getValue(paths.get(i), timestamp)
                : seriesReaderByTimestampList.get(i).getValueInTimestamp(timestamp);
        if (value != null) {
          hasField = true;
          rowInObjects[i] = value;
        }
      }

      if (hasField) {
        hasCachedRow = true;
        cachedRowInObjects = rowInObjects;
        break;
      }
    }
    return hasCachedRow;
  }
}
