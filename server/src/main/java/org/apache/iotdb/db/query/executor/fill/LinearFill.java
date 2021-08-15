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
import org.apache.iotdb.db.metadata.PartialPath;
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
  protected long beforeRange;
  protected long afterRange;
  protected Filter beforeFilter;
  protected Filter afterFilter;
  protected QueryContext context;
  // all measurements sharing the same device as "seriesPath"
  protected Set<String> deviceMeasurements;

  public LinearFill(long beforeRange, long afterRange) {
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  /** Constructor of LinearFill. */
  public LinearFill(TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    this.afterRange = afterRange;
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  public void setBeforeRange(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  public long getAfterRange() {
    return afterRange;
  }

  public void setAfterRange(long afterRange) {
    this.afterRange = afterRange;
  }

  @Override
  public IFill copy() {
    return new LinearFill(dataType, queryTime, beforeRange, afterRange);
  }

  @Override
  void constructFilter() {
    Filter lowerBound =
        beforeRange == -1
            ? TimeFilter.gtEq(Long.MIN_VALUE)
            : TimeFilter.gtEq(queryTime - beforeRange);
    Filter upperBound =
        afterRange == -1
            ? TimeFilter.ltEq(Long.MAX_VALUE)
            : TimeFilter.ltEq(queryTime + afterRange);
    // [queryTIme - beforeRange, queryTime + afterRange]
    beforeFilter = FilterFactory.and(lowerBound, TimeFilter.ltEq(queryTime));
    afterFilter = FilterFactory.and(TimeFilter.gtEq(queryTime), upperBound);
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
    this.queryTime = queryTime;
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
    if (beforePair.getValue() == null || beforePair.getTimestamp() == queryTime) {
      beforePair.setTimestamp(queryTime);
      return beforePair;
    }

    // on after data or after data is out of range
    if (afterPair.getValue() == null
        || afterPair.getTimestamp() < queryTime
        || (afterRange != -1 && afterPair.getTimestamp() > queryTime + afterRange)) {
      return new TimeValuePair(queryTime, null);
    }

    return average(beforePair, afterPair);
  }

  protected TimeValuePair calculatePrecedingPoint()
      throws QueryProcessException, StorageEngineException, IOException {
    QueryDataSource dataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, beforeFilter);
    LastPointReader lastReader =
        new LastPointReader(
            seriesPath, dataType, deviceMeasurements, context, dataSource, queryTime, beforeFilter);

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
    double beforeTimeLength = (double) (queryTime - beforePair.getTimestamp());
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
    beforePair.setTimestamp(queryTime);
    return beforePair;
  }
}
