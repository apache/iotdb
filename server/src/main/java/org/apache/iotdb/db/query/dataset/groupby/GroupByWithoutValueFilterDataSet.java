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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
import org.apache.iotdb.tsfile.read.common.IOMonitor2.DataSetType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private static final Logger logger =
      LoggerFactory.getLogger(GroupByWithoutValueFilterDataSet.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private Map<PartialPath, GroupByExecutor> pathExecutors = new HashMap<>();

  /**
   * path -> result index for each aggregation
   *
   * <p>e.g.,
   *
   * <p>deduplicated paths : s1, s2, s1 deduplicated aggregations : count, count, sum
   *
   * <p>s1 -> 0, 2 s2 -> 1
   */
  private Map<PartialPath, List<Integer>> resultIndexes = new HashMap<>();

  public GroupByWithoutValueFilterDataSet() {}

  /** constructor. */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimePlan);

    initGroupBy(context, groupByTimePlan);
  }

  protected void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    IExpression expression = groupByTimePlan.getExpression();

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    if (timeFilter == null) {
      throw new QueryProcessException("TimeFilter cannot be null in GroupBy query.");
    }

    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLockAndInitQueryDataSource(
                paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()),
                context,
                timeFilter);
    try {
      // init resultIndexes, group result indexes by path
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        if (!pathExecutors.containsKey(path)) {
          // init GroupByExecutor
          pathExecutors.put(
              path,
              getGroupByExecutor(
                  path,
                  groupByTimePlan.getAllMeasurementsInDevice(path.getDevice()),
                  dataTypes.get(
                      i), // fix bug: here use the aggregation type as the series data type
                  context,
                  timeFilter.copy(),
                  null,
                  groupByTimePlan.isAscending()));
          resultIndexes.put(path, new ArrayList<>());
        }
        resultIndexes.get(path).add(i);
        AggregateResult aggrResult =
            AggregateResultFactory.getAggrResultByName(
                groupByTimePlan.getDeduplicatedAggregations().get(i), dataTypes.get(i), ascending);
        pathExecutors.get(path).addAggregateResult(aggrResult);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  /** Each row correspond to result of a bucket */
  public List<List<AggregateResult>> getAll() throws IOException, QueryProcessException {
    List<List<AggregateResult>> resultsAllBuckets = new ArrayList<>();
    GroupByExecutor executor = null;
    for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
      executor = pathToExecutorEntry.getValue(); // assume only one series here
      break;
    }
    for (long localCurStartTime = startTime;
        localCurStartTime < endTime;
        localCurStartTime += interval) { // not change real curStartTime&curEndTime
      List<AggregateResult> aggregations =
          executor.calcResult(
              localCurStartTime, localCurStartTime + interval, startTime, endTime, interval);
      resultsAllBuckets.add(aggregations); // needs deep copy!!
    }

    // make the next hasNextWithoutConstraint() false
    curStartTime = endTime;
    hasCachedTimeInterval = false;

    return resultsAllBuckets;
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (CONFIG.getEnableTri().equals("MinMax")) {
      return nextWithoutConstraintTri_MinMax();
    }
    //    } else if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
    //      // TODO
    //    } else if (CONFIG.getEnableTri().equals("M4LTTB")) {
    //      // TODO
    //    } else if (CONFIG.getEnableTri().equals("LTTB")) {
    //      // TODO
    //    } else if (CONFIG.getEnableTri().equals("ILTS")) {
    //      // TODO
    //    }
    else {
      return nextWithoutConstraint_raw();
    }
  }

  public RowRecord nextWithoutConstraintTri_MinMax() throws IOException {
    RowRecord record;
    try {
      GroupByExecutor executor = null;
      for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
        executor = pathToExecutorEntry.getValue(); // assume only one series here
        break;
      }

      // concat results into a string
      record = new RowRecord(0);
      StringBuilder series = new StringBuilder();

      for (long localCurStartTime = startTime;
          localCurStartTime < endTime;
          localCurStartTime += interval) { // not change real curStartTime&curEndTime
        // attention the returned aggregations need deep copy if using directly
        List<AggregateResult> aggregations =
            executor.calcResult(
                localCurStartTime,
                localCurStartTime + interval,
                startTime,
                endTime,
                interval); // attention
        for (AggregateResult aggregation : aggregations) {
          // Each row correspond to (bucketLeftBound, minV[bottomT], maxV[topT]) of a MinMax bucket
          series.append(aggregation.getResult()).append(",");
        }
      }

      // MIN_MAX_INT64 this type for field.setBinaryV(new Binary(value.toString()))
      record.addField(series, TSDataType.MIN_MAX_INT64);

    } catch (QueryProcessException e) {
      logger.error("GroupByWithoutValueFilterDataSet execute has error", e);
      throw new IOException(e.getMessage(), e);
    }

    // in the end, make the next hasNextWithoutConstraint() false
    // as we already fetch all here
    curStartTime = endTime;
    hasCachedTimeInterval = false;

    return record;
  }

  public RowRecord nextWithoutConstraint_raw() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException(
          "need to call hasNext() before calling next() " + "in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }

    AggregateResult[] fields = new AggregateResult[paths.size()];

    try {
      for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
        GroupByExecutor executor = pathToExecutorEntry.getValue();
        //        long start = System.nanoTime();
        List<AggregateResult> aggregations =
            executor.calcResult(curStartTime, curEndTime, startTime, endTime, interval);
        //        IOMonitor.incTotalTime(System.nanoTime() - start);
        for (int i = 0; i < aggregations.size(); i++) {
          int resultIndex = resultIndexes.get(pathToExecutorEntry.getKey()).get(i);
          fields[resultIndex] = aggregations.get(i);
        }
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByWithoutValueFilterDataSet execute has error", e);
      throw new IOException(e.getMessage(), e);
    }

    for (AggregateResult res : fields) {
      if (res == null) {
        record.addField(null);
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(Path path, int i) throws IOException {
    Pair<Long, Object> result = null;
    long nextStartTime = curStartTime;
    long nextEndTime;
    do {
      nextStartTime -= slidingStep;
      if (nextStartTime >= startTime) {
        nextEndTime = Math.min(nextStartTime + interval, endTime);
      } else {
        return null;
      }
      result = pathExecutors.get(path).peekNextNotNullValue(nextStartTime, nextEndTime);
    } while (result == null);
    return result;
  }

  protected GroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    if (CONFIG.getEnableTri().equals("MinMax")) {
      // TODO
      return new LocalGroupByExecutorTri_MinMax(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
      // TODO
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("M4LTTB")) {
      // TODO
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("LTTB")) {
      // TODO
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("ILTS")) {
      // TODO
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    }
    // deprecated below
    else if (CONFIG.isEnableCPV()) {
      if (TSFileDescriptor.getInstance().getConfig().isEnableMinMaxLSM()) { // MinMax-LSM
        IOMonitor2.dataSetType =
            DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_EnableMinMaxLSM;
        return new LocalGroupByExecutor4MinMax(
            path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
      } else { // M4-LSM
        if (TSFileDescriptor.getInstance().getConfig().isUseTimeIndex()
            && TSFileDescriptor.getInstance().getConfig().isUseValueIndex()) {
          IOMonitor2.dataSetType =
              DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_UseIndex;
        } else if (!TSFileDescriptor.getInstance().getConfig().isUseTimeIndex()
            && TSFileDescriptor.getInstance().getConfig().isUseValueIndex()) {
          IOMonitor2.dataSetType =
              DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_NoTimeIndex;
        } else if (TSFileDescriptor.getInstance().getConfig().isUseTimeIndex()
            && !TSFileDescriptor.getInstance().getConfig().isUseValueIndex()) {
          IOMonitor2.dataSetType =
              DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_NoValueIndex;
        } else {
          IOMonitor2.dataSetType =
              DataSetType
                  .GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_NoTimeValueIndex;
        }
        return new LocalGroupByExecutor4CPV(
            path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
      }
    } else { // enableCPV=false
      if (TSFileDescriptor.getInstance().getConfig().isUseStatistics()) {
        IOMonitor2.dataSetType =
            DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor_UseStatistics;
      } else {
        IOMonitor2.dataSetType =
            DataSetType.GroupByWithoutValueFilterDataSet_LocalGroupByExecutor_NotUseStatistics;
      }
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    }
  }
}
