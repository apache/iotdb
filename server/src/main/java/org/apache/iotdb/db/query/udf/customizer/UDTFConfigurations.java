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

package org.apache.iotdb.db.query.udf.customizer;

import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.DatetimeUtils.DurationUnit;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class UDTFConfigurations extends UDFConfigurations {

  /**
   *
   */
  public enum DataPointAccessStrategy {
    FETCH_BY_POINT,
    FETCH_BY_TIME_WINDOW,
    FETCH_BY_SIZE_LIMITED_WINDOW,
    RANDOM_ACCESS_TO_OVERALL_DATA,
  }

  /**
   *
   */
  public enum RowRecordAccessStrategy {
    FETCH_BY_ROW,
    FETCH_BY_TIME_WINDOW,
    FETCH_BY_SIZE_LIMITED_WINDOW,
    RANDOM_ACCESS_TO_OVERALL_DATA,
  }

  /**
   *
   */
  protected List<Path> paths;

  /**
   *
   */
  protected DataPointAccessStrategy[] dataPointAccessStrategies;

  /**
   *
   */
  protected Integer[] dataPointBatchSizeLimits;

  /**
   *
   */
  protected Long[] dataPointBatchTimeWindowsInNanoseconds;

  /**
   *
   */
  protected HashMap<String, List<Integer>> tablets;

  /**
   *
   */
  protected HashMap<String, RowRecordAccessStrategy> rowRecordAccessStrategies;

  /**
   *
   */
  protected HashMap<String, Integer> rowRecordBatchSizeLimits;

  /**
   *
   */
  protected HashMap<String, Long> rowRecordBatchTimeWindowsInNanoseconds;

  public UDTFConfigurations(List<Path> paths) {
    this.paths = paths;
    int seriesNumber = paths.size();
    dataPointAccessStrategies = new DataPointAccessStrategy[seriesNumber];
    dataPointBatchSizeLimits = new Integer[seriesNumber];
    dataPointBatchTimeWindowsInNanoseconds = new Long[seriesNumber];
    tablets = new HashMap<>();
    rowRecordAccessStrategies = new HashMap<>();
    rowRecordBatchSizeLimits = new HashMap<>();
    rowRecordBatchTimeWindowsInNanoseconds = new HashMap<>();
  }

  public UDTFConfigurations setColumnHeader(String columnHeader) {
    this.columnHeader = columnHeader;
    return this;
  }

  public UDTFConfigurations setOutputDataType(TSDataType outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }

  public UDTFConfigurations setDataPointAccessStrategies(int seriesIndex,
      DataPointAccessStrategy dataPointAccessStrategy) {
    dataPointAccessStrategies[seriesIndex] = dataPointAccessStrategy;
    return this;
  }

  public UDTFConfigurations setBatchSizeLimit(int seriesIndex, int sizeLimit) {
    dataPointBatchSizeLimits[seriesIndex] = sizeLimit;
    return this;
  }

  public UDFConfigurations setBatchTimeWindow(int seriesIndex, long timeWindow, DurationUnit unit) {
    dataPointBatchTimeWindowsInNanoseconds[seriesIndex] = DatetimeUtils
        .convertDurationStrToLong(timeWindow, unit.toString(), "ns");
    return this;
  }

  /**
   * @param tabletName
   * @param indexes
   */
  public UDTFConfigurations mergeSeriesIntoTablet(String tabletName, List<Integer> indexes) {
    tablets.put(tabletName, indexes);
    return this;
  }

  /**
   * @param tabletName
   * @param rowRecordAccessStrategy
   */
  public UDTFConfigurations setRowRecordAccessStrategies(String tabletName,
      RowRecordAccessStrategy rowRecordAccessStrategy) {
    rowRecordAccessStrategies.put(tabletName, rowRecordAccessStrategy);
    return this;
  }

  /**
   * @param tabletName
   * @param sizeLimit
   */
  public UDTFConfigurations setBatchSizeLimit(String tabletName, int sizeLimit) {
    rowRecordBatchSizeLimits.put(tabletName, sizeLimit);
    return this;
  }

  /**
   * @param tabletName
   * @param timeWindow
   * @param unit
   */
  public UDTFConfigurations setBatchTimeWindow(String tabletName, long timeWindow,
      DurationUnit unit) {
    rowRecordBatchTimeWindowsInNanoseconds
        .put(tabletName, DatetimeUtils.convertDurationStrToLong(timeWindow, unit.toString(), "ns"));
    return this;
  }

  public DataPointAccessStrategy[] getDataPointAccessStrategies() {
    return dataPointAccessStrategies;
  }

  public Integer[] getDataPointBatchSizeLimits() {
    return dataPointBatchSizeLimits;
  }

  public Long[] getDataPointBatchTimeWindowsInNanoseconds() {
    return dataPointBatchTimeWindowsInNanoseconds;
  }

  public HashMap<String, List<Integer>> getTablets() {
    return tablets;
  }

  public HashMap<String, RowRecordAccessStrategy> getRowRecordAccessStrategies() {
    return rowRecordAccessStrategies;
  }

  public HashMap<String, Integer> getRowRecordBatchSizeLimits() {
    return rowRecordBatchSizeLimits;
  }

  public HashMap<String, Long> getRowRecordBatchTimeWindowsInNanoseconds() {
    return rowRecordBatchTimeWindowsInNanoseconds;
  }

  public void check() throws QueryProcessException {
    // for data point access
    for (int i = 0; i < dataPointAccessStrategies.length; ++i) {
      if (dataPointAccessStrategies[i] == null) {
        continue;
      }
      switch (dataPointAccessStrategies[i]) {
        case FETCH_BY_SIZE_LIMITED_WINDOW:
          checkWindowSize(dataPointBatchSizeLimits[i], paths.get(i).toString());
          break;
        case FETCH_BY_TIME_WINDOW:
          checkWindowTime(dataPointBatchTimeWindowsInNanoseconds[i], paths.get(i).toString());
          break;
      }
    }

    // for row record access
    for (String tabletName : tablets.keySet()) {
      RowRecordAccessStrategy strategy = rowRecordAccessStrategies.get(tabletName);
      if (strategy == null) {
        throw new QueryProcessException("Row record access strategy is not set for tablet %s.");
      }
      switch (strategy) {
        case FETCH_BY_SIZE_LIMITED_WINDOW:
          checkWindowSize(rowRecordBatchSizeLimits.get(tabletName), tabletName);
          break;
        case FETCH_BY_TIME_WINDOW:
          checkWindowTime(rowRecordBatchTimeWindowsInNanoseconds.get(tabletName), tabletName);
          break;
      }
    }
  }

  private static void checkWindowSize(Integer size, String id) throws QueryProcessException {
    if (size == null) {
      throw new QueryProcessException(String.format("Batch size is not set for %s.", id));
    }
    if (size <= 0) {
      throw new QueryProcessException(
          String.format("Batch size for %s should be positive. Current Batch size: %d.", id, size));
    }
  }

  private static void checkWindowTime(Long time, String id) throws QueryProcessException {
    if (time == null) {
      throw new QueryProcessException(String.format("Time window is not set for %s.", id));
    }
    if (time <= 0) {
      throw new QueryProcessException(String
          .format("Time window for %s should be positive. Current time window: %d ns.", id, time));
    }
  }
}
