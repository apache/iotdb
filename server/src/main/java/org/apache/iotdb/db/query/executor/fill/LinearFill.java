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

package org.apache.iotdb.db.query.executor.fill;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.FirstValueAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.MinTimeAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LinearFill extends IFill {

  protected PartialPath seriesPath;
  protected Filter beforeFilter;
  protected Filter afterFilter;
  protected QueryContext context;
  // all measurements sharing the same device as "seriesPath"
  protected Set<String> deviceMeasurements;

  public LinearFill(long beforeRange, long afterRange) {
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  public LinearFill(String beforeStr, String afterStr) {
    this.beforeRange = DatetimeUtils.convertDurationStrToLong(beforeStr);
    this.afterRange = DatetimeUtils.convertDurationStrToLong(afterStr);
    if (beforeStr.toLowerCase().contains("mo")) {
      this.isBeforeByMonth = true;
    }
    if (afterStr.toLowerCase().contains("mo")) {
      this.isAfterByMonth = true;
    }
  }

  /** Constructor of LinearFill. */
  public LinearFill(TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  public LinearFill(
      TSDataType dataType,
      long queryTime,
      long beforeRange,
      long afterRange,
      boolean isBeforeByMonth,
      boolean isAfterByMonth) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
    this.isBeforeByMonth = isBeforeByMonth;
    this.isAfterByMonth = isAfterByMonth;
  }

  public LinearFill() {}

  @Override
  public IFill copy() {
    return new LinearFill(
        dataType, queryStartTime, beforeRange, afterRange, isBeforeByMonth, isAfterByMonth);
  }

  @Override
  void constructFilter() {
    Filter lowerBound =
        beforeRange == -1
            ? TimeFilter.gtEq(Long.MIN_VALUE)
            : TimeFilter.gtEq(queryStartTime - beforeRange);
    Filter upperBound =
        afterRange == -1
            ? TimeFilter.ltEq(Long.MAX_VALUE)
            : TimeFilter.ltEq(queryStartTime + afterRange);
    // [queryTIme - beforeRange, queryTime + afterRange]
    beforeFilter = FilterFactory.and(lowerBound, TimeFilter.ltEq(queryStartTime));
    afterFilter = FilterFactory.and(TimeFilter.gtEq(queryStartTime), upperBound);
  }

  @Override
  public void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> sensors,
      QueryContext context) {
    this.seriesPath = path;
    this.dataType = dataType;
    this.queryStartTime = queryTime;
    this.context = context;
    this.deviceMeasurements = sensors;
    constructFilter();
  }

  @Override
  public TimeValuePair getFillResult()
      throws IOException, QueryProcessException, StorageEngineException {

    TimeValuePair beforePair = calculatePrecedingPoint();
    TimeValuePair afterPair = calculateSucceedingPoint();

    // no before data or has data on the query timestamp
    if (beforePair.getValue() == null || beforePair.getTimestamp() == queryStartTime) {
      beforePair.setTimestamp(queryStartTime);
      return beforePair;
    }

    // on after data or after data is out of range
    if (afterPair.getValue() == null
        || afterPair.getTimestamp() < queryStartTime
        || (afterRange != -1 && afterPair.getTimestamp() > queryStartTime + afterRange)) {
      return new TimeValuePair(queryStartTime, null);
    }

    return average(beforePair, afterPair);
  }

  protected TimeValuePair calculatePrecedingPoint()
      throws QueryProcessException, StorageEngineException, IOException {
    QueryDataSource dataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, beforeFilter);
    LastPointReader lastReader =
        new LastPointReader(
            seriesPath,
            dataType,
            deviceMeasurements,
            context,
            dataSource,
            queryStartTime,
            beforeFilter);

    return lastReader.readLastPoint();
  }

  protected TimeValuePair calculateSucceedingPoint()
      throws IOException, StorageEngineException, QueryProcessException {

    List<AggregateResult> aggregateResultList = new ArrayList<>();
    AggregateResult minTimeResult = new MinTimeAggrResult();
    AggregateResult firstValueResult = new FirstValueAggrResult(dataType);
    aggregateResultList.add(minTimeResult);
    aggregateResultList.add(firstValueResult);
    AggregationExecutor.aggregateOneSeries(
        seriesPath,
        deviceMeasurements,
        context,
        afterFilter,
        dataType,
        aggregateResultList,
        null,
        null);

    return convertToResult(minTimeResult, firstValueResult);
  }

  protected TimeValuePair convertToResult(
      AggregateResult minTimeResult, AggregateResult firstValueResult) {
    TimeValuePair result = new TimeValuePair(0, null);
    if (minTimeResult.getResult() != null) {
      long timestamp = (long) (minTimeResult.getResult());
      result.setTimestamp(timestamp);
    }
    if (firstValueResult.getResult() != null) {
      Object value = firstValueResult.getResult();
      result.setValue(TsPrimitiveType.getByType(dataType, value));
    }
    return result;
  }

  // returns the average of two points
  private TimeValuePair average(TimeValuePair beforePair, TimeValuePair afterPair)
      throws UnSupportedFillTypeException {
    double totalTimeLength = (double) afterPair.getTimestamp() - beforePair.getTimestamp();
    double beforeTimeLength = (double) (queryStartTime - beforePair.getTimestamp());
    switch (dataType) {
      case INT32:
        int startIntValue = beforePair.getValue().getInt();
        int endIntValue = afterPair.getValue().getInt();
        int fillIntValue =
            startIntValue
                + (int)
                    ((double) (endIntValue - startIntValue) / totalTimeLength * beforeTimeLength);
        beforePair.setValue(TsPrimitiveType.getByType(TSDataType.INT32, fillIntValue));
        break;
      case INT64:
        long startLongValue = beforePair.getValue().getLong();
        long endLongValue = afterPair.getValue().getLong();
        long fillLongValue =
            startLongValue
                + (long)
                    ((double) (endLongValue - startLongValue) / totalTimeLength * beforeTimeLength);
        beforePair.setValue(TsPrimitiveType.getByType(TSDataType.INT64, fillLongValue));
        break;
      case FLOAT:
        float startFloatValue = beforePair.getValue().getFloat();
        float endFloatValue = afterPair.getValue().getFloat();
        float fillFloatValue =
            startFloatValue
                + (float) ((endFloatValue - startFloatValue) / totalTimeLength * beforeTimeLength);
        beforePair.setValue(TsPrimitiveType.getByType(TSDataType.FLOAT, fillFloatValue));
        break;
      case DOUBLE:
        double startDoubleValue = beforePair.getValue().getDouble();
        double endDoubleValue = afterPair.getValue().getDouble();
        double fillDoubleValue =
            startDoubleValue
                + ((endDoubleValue - startDoubleValue) / totalTimeLength * beforeTimeLength);
        beforePair.setValue(TsPrimitiveType.getByType(TSDataType.DOUBLE, fillDoubleValue));
        break;
      default:
        throw new UnSupportedFillTypeException(dataType);
    }
    beforePair.setTimestamp(queryStartTime);
    return beforePair;
  }

  public TimeValuePair averageWithTimeAndDataType(
      TimeValuePair beforePair, TimeValuePair afterPair, long queryTime, TSDataType tsDataType)
      throws UnSupportedFillTypeException {
    this.queryStartTime = queryTime;
    this.dataType = tsDataType;
    return average(beforePair, afterPair);
  }
}
