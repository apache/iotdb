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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.io.IOException;
import java.util.Set;

public class PreviousFill extends IFill {

  private PartialPath seriesPath;
  private QueryContext context;
  private Set<String> allSensors;
  private Filter timeFilter;

  private boolean untilLast;

  public PreviousFill(TSDataType dataType, long queryTime, long beforeRange) {
    this(dataType, queryTime, beforeRange, false, false);
  }

  public PreviousFill(long beforeRange) {
    this(beforeRange, false);
  }

  public PreviousFill(String beforeStr) {
    this(beforeStr, false);
  }

  public PreviousFill(long beforeRange, boolean untilLast) {
    this.beforeRange = beforeRange;
    this.untilLast = untilLast;
  }

  public PreviousFill(String beforeStr, boolean untilLast) {
    this.beforeRange = DateTimeUtils.convertDurationStrToLong(beforeStr);
    this.untilLast = untilLast;
    if (beforeStr.toLowerCase().contains("mo")) {
      this.isBeforeByMonth = true;
    }
  }

  public PreviousFill(
      TSDataType dataType,
      long queryStartTime,
      long beforeRange,
      boolean untilLast,
      boolean isBeforeByMonth) {
    super(dataType, queryStartTime);
    this.beforeRange = beforeRange;
    this.untilLast = untilLast;
    this.isBeforeByMonth = isBeforeByMonth;
  }

  @Override
  public IFill copy() {
    return new PreviousFill(dataType, queryStartTime, beforeRange, untilLast, isBeforeByMonth);
  }

  @Override
  void constructFilter() {
    Filter lowerBound =
        beforeRange == -1
            ? TimeFilter.gtEq(Long.MIN_VALUE)
            : TimeFilter.gtEq(queryStartTime - beforeRange);
    // time in [queryTime - beforeRange, queryTime]
    timeFilter = FilterFactory.and(lowerBound, TimeFilter.ltEq(queryStartTime));
  }

  @Override
  public void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> sensors,
      QueryContext context)
      throws QueryProcessException, StorageEngineException {
    this.seriesPath = path;
    this.dataType = dataType;
    this.context = context;
    this.queryStartTime = queryTime;
    this.allSensors = sensors;
    constructFilter();
  }

  @Override
  public TimeValuePair getFillResult()
      throws IOException, QueryProcessException, StorageEngineException {
    // for the parameter "ascending": true or false both ok here,
    // because LastPointReader will do itself sort logic instead of depending on fillOrderIndex.
    QueryDataSource dataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, false);
    // update filter by TTL
    timeFilter = dataSource.updateFilterUsingTTL(timeFilter);
    LastPointReader lastReader =
        new LastPointReader(
            seriesPath, dataType, allSensors, context, dataSource, queryStartTime, timeFilter);

    return lastReader.readLastPoint();
  }

  public boolean isUntilLast() {
    return untilLast;
  }

  public void setUntilLast(boolean untilLast) {
    this.untilLast = untilLast;
  }
}
