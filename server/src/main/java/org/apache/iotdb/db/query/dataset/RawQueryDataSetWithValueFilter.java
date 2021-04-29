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
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RawQueryDataSetWithValueFilter extends QueryDataSet implements UDFInputDataSet {

  private final TimeGenerator timeGenerator;
  private final List<IReaderByTimestamp> seriesReaderByTimestampList;
  private final List<Boolean> cached;

  private List<RowRecord> cachedRowRecords = new ArrayList<>();

  /** Used for UDF. */
  private List<Object[]> cachedRowInObjects = new ArrayList<>();

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
    if (!cachedRowRecords.isEmpty()) {
      return true;
    }
    return cacheRowRecords();
  }

  /** @return the first record of cached rows or null if there is no more data */
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (cachedRowRecords.isEmpty() && !cacheRowRecords()) {
      return null;
    }
    return cachedRowRecords.remove(cachedRowRecords.size() - 1);
  }

  /**
   * Cache row records
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecords() throws IOException {
    int cachedTimeCnt = 0;
    long[] cachedTimeArray = new long[fetchSize];
    // TODO: LIMIT constraint
    // 1. fill time array from time Generator
    while (timeGenerator.hasNext() && cachedTimeCnt < fetchSize) {
      cachedTimeArray[cachedTimeCnt++] = timeGenerator.next();
    }
    if (cachedTimeCnt == 0) {
      return false;
    }
    RowRecord[] rowRecords = new RowRecord[cachedTimeCnt];
    for (int i = 0; i < cachedTimeCnt; i++) {
      rowRecords[i] = new RowRecord(cachedTimeArray[i]);
    }

    boolean[] hasField = new boolean[cachedTimeCnt];
    // 2. fetch results of each time series using time array
    for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
      Object[] results;
      // get value from readers in time generator
      if (cached.get(i)) {
        results = timeGenerator.getValues(paths.get(i));
      } else {
        results =
            seriesReaderByTimestampList
                .get(i)
                .getValuesInTimestamps(cachedTimeArray, cachedTimeCnt);
      }

      // 3. use values in results to fill row record
      for (int j = 0; j < cachedTimeCnt; j++) {
        if (results[j] == null) {
          rowRecords[j].addField(null);
        } else {
          hasField[j] = true;
          if (dataTypes.get(i) == TSDataType.VECTOR) {
            TsPrimitiveType[] result = (TsPrimitiveType[]) results[j];
            rowRecords[j].addField(result[0].getValue(), result[0].getDataType());
          } else {
            rowRecords[j].addField(results[j], dataTypes.get(i));
          }
        }
      }
    }
    // 4. remove rowRecord if all values in one timestamp are null
    // traverse in reversed order to get element efficiently
    for (int i = cachedTimeCnt - 1; i >= 0; i--) {
      if (hasField[i]) {
        cachedRowRecords.add(rowRecords[i]);
      }
    }

    // 5. check whether there is next row record
    if (cachedRowRecords.isEmpty() && timeGenerator.hasNext()) {
      // Note: This may leads to a deep stack if much rowRecords are empty
      return cacheRowRecords();
    }
    return !cachedRowRecords.isEmpty();
  }

  @Override
  public boolean hasNextRowInObjects() throws IOException {
    if (!cachedRowInObjects.isEmpty()) {
      return true;
    }
    return cacheRowInObjects();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    if (cachedRowInObjects.isEmpty() && !cacheRowInObjects()) {
      // values + timestamp
      return new Object[seriesReaderByTimestampList.size() + 1];
    }

    return cachedRowInObjects.remove(cachedRowInObjects.size() - 1);
  }

  private boolean cacheRowInObjects() throws IOException {
    int cachedTimeCnt = 0;
    long[] cachedTimeArray = new long[fetchSize];

    // TODO: LIMIT constraint
    // 1. fill time array from time Generator
    while (timeGenerator.hasNext() && cachedTimeCnt < fetchSize) {
      cachedTimeArray[cachedTimeCnt++] = timeGenerator.next();
    }
    if (cachedTimeCnt == 0) {
      return false;
    }

    Object[][] rowsInObject = new Object[cachedTimeCnt][seriesReaderByTimestampList.size() + 1];
    for (int i = 0; i < cachedTimeCnt; i++) {
      rowsInObject[i][seriesReaderByTimestampList.size()] = cachedTimeArray[i];
    }

    boolean[] hasField = new boolean[cachedTimeCnt];
    // 2. fetch results of each time series using time array
    for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
      Object[] results;
      // get value from readers in time generator
      if (cached.get(i)) {
        results = timeGenerator.getValues(paths.get(i));
      } else {
        results =
            seriesReaderByTimestampList
                .get(i)
                .getValuesInTimestamps(cachedTimeArray, cachedTimeCnt);
      }

      // 3. use values in results to fill row record
      for (int j = 0; j < cachedTimeCnt; j++) {
        if (results[j] != null) {
          hasField[j] = true;
          rowsInObject[j][i] = results[j];
        }
      }
    }
    // 4. remove rowRecord if all values in one timestamp are null
    // traverse in reversed order to get element efficiently
    for (int i = cachedTimeCnt - 1; i >= 0; i--) {
      if (hasField[i]) {
        cachedRowInObjects.add(rowsInObject[i]);
      }
    }

    // 5. check whether there is next row record
    if (cachedRowInObjects.isEmpty() && timeGenerator.hasNext()) {
      // Note: This may leads to a deep stack if much rowRecords are empty
      return cacheRowInObjects();
    }
    return !cachedRowInObjects.isEmpty();
  }
}
