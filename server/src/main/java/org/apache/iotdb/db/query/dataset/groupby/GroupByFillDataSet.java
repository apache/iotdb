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
package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GroupByFillDataSet extends QueryDataSet {

  private static final Logger logger = LoggerFactory.getLogger(GroupByFillDataSet.class);

  private GroupByEngineDataSet groupByEngineDataSet;
  private final LastQueryExecutor lastQueryExecutor;
  private Map<TSDataType, IFill> fillTypes;
  // the first value for each time series
  private Object[] previousValue;
  private long[] previousTime;
  // last timestamp for each time series
  private long[] lastTimeArray;
  private TimeValuePair[] firstNotNullTV;
  private PreviousFill[] previousFillExecutors;
  private boolean isPeekEnded = false;

  public GroupByFillDataSet(
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      GroupByEngineDataSet groupByEngineDataSet,
      Map<TSDataType, IFill> fillTypes,
      QueryContext context,
      GroupByTimeFillPlan groupByFillPlan,
      LastQueryExecutor lastQueryExecutor)
      throws StorageEngineException, IOException, QueryProcessException {
    super(new ArrayList<>(paths), dataTypes, groupByFillPlan.isAscending());
    this.groupByEngineDataSet = groupByEngineDataSet;
    this.lastQueryExecutor = lastQueryExecutor;
    this.fillTypes = fillTypes;

    initPreviousParis(context, groupByFillPlan);
    initLastTimeArray(context, groupByFillPlan);
  }

  private void initPreviousParis(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    previousValue = new Object[paths.size()];
    previousTime = new long[paths.size()];
    firstNotNullTV = new TimeValuePair[paths.size()];
    previousFillExecutors = new PreviousFill[paths.size()];

    long lowerBound = Long.MAX_VALUE;
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = (PartialPath) paths.get(i);
      TSDataType dataType = dataTypes.get(i);
      PreviousFill fill;
      long beforeRange;
      if (fillTypes.containsKey(dataType)) {
        beforeRange = ((PreviousFill) fillTypes.get(dataType)).getBeforeRange();
        fill =
            new PreviousFill(
                dataType,
                groupByEngineDataSet.getStartTime(),
                beforeRange,
                ((PreviousFill) fillTypes.get(dataType)).isUntilLast());
      } else {
        beforeRange = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
        fill = new PreviousFill(dataType, groupByEngineDataSet.getStartTime(), beforeRange);
      }
      fill.configureFill(
          path,
          dataType,
          groupByEngineDataSet.getStartTime(),
          groupByFillPlan.getAllMeasurementsInDevice(path.getDevice()),
          context);
      previousFillExecutors[i] = fill;

      lowerBound =
          Math.min(
              lowerBound,
              beforeRange == -1
                  ? Long.MIN_VALUE
                  : groupByEngineDataSet.getStartTime() - beforeRange);
    }

    Filter timeFilter =
        FilterFactory.and(
            TimeFilter.gtEq(lowerBound), TimeFilter.ltEq(groupByEngineDataSet.getStartTime()));

    Pair<List<StorageGroupProcessor>, Map<StorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance()
                .mergeLock(
                    paths.stream().map(path -> (PartialPath) path).collect(Collectors.toList()));
    List<StorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<StorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    for (int i = 0; i < paths.size(); i++) {
      PreviousFill fill = previousFillExecutors[i];
      firstNotNullTV[i] = fill.getFillResult();
      TimeValuePair timeValuePair = firstNotNullTV[i];
      previousValue[i] = null;
      previousTime[i] = Long.MAX_VALUE;
      if (ascending && timeValuePair != null && timeValuePair.getValue() != null) {
        previousValue[i] = timeValuePair.getValue().getValue();
        previousTime[i] = timeValuePair.getTimestamp();
      }
    }
  }

  private void initLastTimeArray(QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws IOException, StorageEngineException, QueryProcessException {
    lastTimeArray = new long[paths.size()];
    Arrays.fill(lastTimeArray, Long.MAX_VALUE);
    List<PartialPath> seriesPaths = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      seriesPaths.add((PartialPath) paths.get(i));
    }
    List<Pair<Boolean, TimeValuePair>> lastValueContainer =
        lastQueryExecutor.calculateLastPairForSeries(
            seriesPaths, dataTypes, context, null, groupByFillPlan);
    for (int i = 0; i < lastValueContainer.size(); i++) {
      if (Boolean.TRUE.equals(lastValueContainer.get(i).left)) {
        lastTimeArray[i] = lastValueContainer.get(i).right.getTimestamp();
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return groupByEngineDataSet.hasNextWithoutConstraint();
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public RowRecord nextWithoutConstraint() throws IOException {
    RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();

    for (int i = 0; i < paths.size(); i++) {
      Field field = rowRecord.getFields().get(i);
      // current group by result is null
      if (field == null || field.getDataType() == null) {
        TSDataType tsDataType = dataTypes.get(i);
        // for desc query peek previous time and value
        if (!ascending && !isPeekEnded && !canUseCacheData(rowRecord, tsDataType, i)) {
          fillCache(i);
        }

        if (canUseCacheData(rowRecord, tsDataType, i)) {
          rowRecord.getFields().set(i, Field.getField(previousValue[i], tsDataType));
        }
      } else {
        // use now value update previous value
        previousValue[i] = field.getObjectValue(field.getDataType());
        previousTime[i] = rowRecord.getTimestamp();
      }
    }
    return rowRecord;
  }

  private void fillCache(int i) throws IOException {
    Pair<Long, Object> data = groupByEngineDataSet.peekNextNotNullValue(paths.get(i), i);
    if (data == null) {
      isPeekEnded = true;
      previousTime[i] = Long.MIN_VALUE;
      previousValue[i] = null;
      if (!firstCacheIsEmpty(i)) {
        previousValue[i] = firstNotNullTV[i].getValue().getValue();
        previousTime[i] = firstNotNullTV[i].getTimestamp();
      }
    } else {
      previousValue[i] = data.right;
      previousTime[i] = data.left;
    }
  }

  // the previous value is not null
  // and (fill type is not previous until last or now time is before last time)
  // and (previous before range is not limited or previous before range contains the previous
  // interval)
  private boolean canUseCacheData(RowRecord rowRecord, TSDataType tsDataType, int i) {
    PreviousFill previousFill = (PreviousFill) fillTypes.get(tsDataType);
    return !cacheIsEmpty(i)
        && satisfyTime(rowRecord, tsDataType, previousFill, lastTimeArray[i])
        && satisfyRange(tsDataType, previousFill)
        && isIncreasingTime(rowRecord, previousTime[i]);
  }

  private boolean isIncreasingTime(RowRecord rowRecord, long time) {
    return rowRecord.getTimestamp() >= time;
  }

  private boolean satisfyTime(
      RowRecord rowRecord, TSDataType tsDataType, PreviousFill previousFill, long lastTime) {
    return (fillTypes.containsKey(tsDataType) && !previousFill.isUntilLast())
        || rowRecord.getTimestamp() <= lastTime;
  }

  private boolean satisfyRange(TSDataType tsDataType, PreviousFill previousFill) {
    return !fillTypes.containsKey(tsDataType)
        || previousFill.getBeforeRange() < 0
        || previousFill.getBeforeRange() >= groupByEngineDataSet.interval;
  }

  private boolean cacheIsEmpty(int i) {
    return previousValue[i] == null;
  }

  private boolean firstCacheIsEmpty(int i) {
    return firstNotNullTV[i] == null || firstNotNullTV[i].getValue() == null;
  }
}
