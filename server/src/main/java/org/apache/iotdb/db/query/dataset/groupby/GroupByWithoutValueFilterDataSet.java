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
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.read.common.IOMonitor2;
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

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
      return nextWithoutConstraintTri_MinMaxLTTB();
    } else if (!CONFIG.getEnableTri().equals("")) {
      return nextWithoutConstraintTri_allInOne();
    } else {
      return nextWithoutConstraint_raw();
    }

    //    if (CONFIG.getEnableTri().equals("MinMax")
    //        || CONFIG.getEnableTri().equals("M4")
    //        || CONFIG.getEnableTri().equals("LTTB")
    //        || CONFIG.getEnableTri().equals("ILTS")
    //        || CONFIG.getEnableTri().equals("SimPiece")
    //        || CONFIG.getEnableTri().equals("SC")
    //        || CONFIG.getEnableTri().equals("FSW")
    //        || CONFIG.getEnableTri().equals("Uniform")
    //    ) {
    //      return nextWithoutConstraintTri_allInOne();
    //    } else if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
    //      return nextWithoutConstraintTri_MinMaxLTTB();
    //    } else {
    //      return nextWithoutConstraint_raw();
    //    }
  }

  public RowRecord nextWithoutConstraintTri_allInOne() throws IOException {
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

      // all bucket results as string in value of MinValueAggrResult
      List<AggregateResult> aggregations =
          executor.calcResult(startTime, startTime + interval, startTime, endTime, interval);
      MinMaxInfo minMaxInfo = (MinMaxInfo) aggregations.get(0).getResult();
      series.append(minMaxInfo.val);

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

  public RowRecord nextWithoutConstraintTri_MinMaxLTTB() throws IOException {

    int divide = 2; // one LTTB bucket corresponds to rps/2 MinMax buckets

    long p1t = CONFIG.getP1t();
    double p1v = CONFIG.getP1v();
    long pnt = CONFIG.getPnt();
    double pnv = CONFIG.getPnv();
    int rps = CONFIG.getRps();

    RowRecord record;

    // concat results into a string
    record = new RowRecord(0);
    StringBuilder series = new StringBuilder();

    // First step: get the MinMax preselection result
    List<Long> times = new ArrayList<>();
    List<Double> values = new ArrayList<>();
    LocalGroupByExecutorTri_MinMax executor = null;
    for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
      executor =
          (LocalGroupByExecutorTri_MinMax)
              (pathToExecutorEntry.getValue()); // assume only one series here
      break;
    }
    // get MinMax preselection times&values list
    executor.calcResult(
        startTime, startTime + interval, startTime, endTime, interval, times, values);

    //      for (long localCurStartTime = startTime;
    //          localCurStartTime + interval <= endTime;
    //          // + interval to make the last bucket complete
    //          // e.g, T=11,nout=3,interval=floor(11/3)=3,
    //          // [0,3),[3,6),[6,9), no need incomplete [9,11)
    //          // then the number of buckets must be Math.floor((endTime-startTime)/interval)
    //          localCurStartTime += interval) {
    //        // not change real curStartTime&curEndTime
    //        // attention the returned aggregations need deep copy if using directly
    //        List<AggregateResult> aggregations =
    //            executor.calcResult(
    //                localCurStartTime, localCurStartTime + interval, startTime, endTime,
    // interval);
    //        int c = 0;
    //        for (AggregateResult aggregation : aggregations) {
    //          // ATTENTION only take the first two aggregation fields, which are BPv[BPt],
    // TPv[TPt]
    //          // Each row correspond to (bucketLeftBound, minV[bottomT], maxV[topT]) of a MinMax
    // bucket
    //          MinMaxInfo minMaxInfo = (MinMaxInfo) aggregation.getResult();
    //          if (minMaxInfo == null) {
    //            times.add(null);
    //            values.add(null);
    //          } else {
    //            times.add(minMaxInfo.timestamp);
    //            values.add((Double) minMaxInfo.val);
    //          }
    //          c++;
    //          if (c >= 2) {
    //            // ATTENTION only take the first two aggregation fields, which are BPv[BPt],
    // TPv[TPt]
    //            break;
    //          }
    //        }
    //      }

    // Second step: apply LTTB on the MinMax preselection result
    int N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // MinMax桶数
    int N2 = N1 / (rps / divide);
    series.append(p1v).append("[").append(p1t).append("]").append(",");
    long lt = p1t; // left fixed t
    double lv = p1v; // left fixed v
    int currentBucket = 0;
    for (; currentBucket < N2; currentBucket++) {
      boolean emptyBucket = true;
      for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
        if (times.get(j) != null) {
          emptyBucket = false;
          break;
        }
      }
      if (!emptyBucket) {
        break;
      }
    }
    for (int nextBucket = currentBucket + 1; nextBucket < N2; nextBucket++) {
      boolean emptyBucket = true;
      for (int j = nextBucket * rps; j < (nextBucket + 1) * rps; j++) {
        if (times.get(j) != null) {
          emptyBucket = false;
          break;
        }
      }
      if (emptyBucket) {
        continue;
      }

      double rt = 0;
      double rv = 0;
      int cnt = 0;
      for (int j = nextBucket * rps; j < (nextBucket + 1) * rps; j++) {
        if (times.get(j) != null) {
          IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
          rt += times.get(j);
          rv += (double) values.get(j);
          cnt++;
        }
      }
      if (cnt == 0) {
        throw new IOException("Empty bucket!");
      }
      rt = rt / cnt;
      rv = rv / cnt;

      double maxArea = -1;
      long select_t = -1;
      double select_v = -1;
      for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
        if (times.get(j) != null) {
          IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
          long t = times.get(j);
          double v = values.get(j);
          double area = IOMonitor2.calculateTri(lt, lv, t, v, rt, rv);
          if (area > maxArea) {
            maxArea = area;
            select_t = t;
            select_v = v;
          }
        }
      }
      if (select_t < 0) {
        throw new IOException("something is wrong");
      }
      series.append(select_v).append("[").append(select_t).append("]").append(",");

      currentBucket = nextBucket;
      lt = select_t;
      lv = select_v;
    }

    double maxArea = -1;
    long select_t = -1;
    double select_v = -1;
    for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
      if (times.get(j) != null) {
        IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
        long t = times.get(j);
        double v = values.get(j);
        double area = IOMonitor2.calculateTri(lt, lv, t, v, pnt, pnv); // 全局尾点作为右边固定点
        if (area > maxArea) {
          maxArea = area;
          select_t = t;
          select_v = v;
        }
      }
    }
    if (select_t < 0) {
      throw new IOException("something is wrong");
    }
    series.append(select_v).append("[").append(select_t).append("]").append(",");

    series.append(pnv).append("[").append(pnt).append("]").append(",");

    record.addField(series, TSDataType.MIN_MAX_INT64);

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
        List<AggregateResult> aggregations =
            executor.calcResult(curStartTime, curEndTime, startTime, endTime, interval);
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
      return new LocalGroupByExecutorTri_MinMax(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
      return new LocalGroupByExecutorTri_MinMax(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
      //      return new LocalGroupByExecutorTri_MinMaxPreselection(
      //          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("M4")) {
      return new LocalGroupByExecutorTri_M4(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("LTTB")) {
      return new LocalGroupByExecutorTri_LTTB(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("ILTS")) {
      if (!TSFileDescriptor.getInstance().getConfig().isWriteConvexHull()
          && CONFIG.isAcc_convex()) {
        throw new QueryProcessException("ILTS use convex hull acceleration, which is not written!");
      }
      return new LocalGroupByExecutorTri_ILTS(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("SimPiece")) {
      return new LocalGroupByExecutorTri_SimPiece(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("SC")) {
      return new LocalGroupByExecutorTri_SC(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("FSW")) {
      return new LocalGroupByExecutorTri_FSW(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("Uniform")) {
      return new LocalGroupByExecutorTri_Uniform(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else if (CONFIG.getEnableTri().equals("Visval")) {
      return new LocalGroupByExecutorTri_Visval(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    } else {
      logger.info("No matched enable_tri!");
      return new LocalGroupByExecutor(
          path, allSensors, dataType, context, timeFilter, fileFilter, ascending);
    }
  }
}
