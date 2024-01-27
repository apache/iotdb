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

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    if (CONFIG.getEnableTri().equals("MinMax")) {
      return nextWithoutConstraintTri_MinMax();
    } else if (CONFIG.getEnableTri().equals("MinMaxLTTB")) {
      return nextWithoutConstraintTri_MinMaxLTTB();
    }
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

  public RowRecord nextWithoutConstraintTri_MinMaxLTTB() throws IOException {
    RowRecord record;

    // TODO tmp p1,pn,rps later passed by config
    long p1t = 0;
    double p1v = -1.2079272;
    long pnt = 2100;
    double pnv = -0.0211206;
    int rps = 4;
    int divide = 2; // one LTTB bucket corresponds to rps/2 MinMax buckets

    // concat results into a string
    record = new RowRecord(0);
    StringBuilder series = new StringBuilder();

    try {
      // First step: get the MinMax preselection result
      List<Long> times = new ArrayList<>();
      List<Object> values = new ArrayList<>();
      GroupByExecutor executor = null;
      for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
        executor = pathToExecutorEntry.getValue(); // assume only one series here
        break;
      }
      for (long localCurStartTime = startTime;
          localCurStartTime + interval <= endTime;
          // 注意有等号！
          // + interval to make the last bucket complete
          // e.g, T=11,nout=3,interval=floor(11/3)=3,
          // [0,3),[3,6),[6,9), no need incomplete [9,11)
          // then the number of buckets must be Math.floor((endTime-startTime)/interval)
          localCurStartTime += interval) {
        System.out.println(localCurStartTime);
        // not change real curStartTime&curEndTime
        // attention the returned aggregations need deep copy if using directly
        List<AggregateResult> aggregations =
            executor.calcResult(
                localCurStartTime, localCurStartTime + interval, startTime, endTime, interval);
        for (AggregateResult aggregation : aggregations) {
          // Each row correspond to (bucketLeftBound, minV[bottomT], maxV[topT]) of a MinMax bucket
          MinMaxInfo minMaxInfo = (MinMaxInfo) aggregation.getResult();
          if (minMaxInfo == null) {
            times.add(null);
            values.add(null);
          } else {
            times.add(minMaxInfo.timestamp);
            values.add(minMaxInfo.val);
          }
        }
      }

      // Second step: apply LTTB on the MinMax preselection result
      System.out.println(times);
      System.out.println(values);
      int N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // MinMax桶数
      int N2 = N1 / (rps / divide); // LTTB桶数
      // 全局首点
      series.append(p1v).append("[").append(p1t).append("]").append(",");
      long lt = p1t; // left fixed t
      double lv = p1v; // left fixed v
      // 找第一个不为空的LTTB当前桶
      int currentBucket = 0;
      for (; currentBucket < N2; currentBucket++) {
        boolean emptyBucket = true;
        for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
          // 一个LTTB桶里有rps个MinMax预选点（包含重复和null）
          if (times.get(j) != null) {
            emptyBucket = false; // 只要有一个MinMax预选点不是null，这个LTTB桶就不是空桶
            break;
          }
        }
        if (!emptyBucket) {
          break;
        }
      }
      // 现在找到了不为空的LTTB当前桶，下面找第一个不为空的LTTB右边桶
      for (int nextBucket = currentBucket + 1; nextBucket < N2; nextBucket++) {
        boolean emptyBucket = true;
        for (int j = nextBucket * rps; j < (nextBucket + 1) * rps; j++) {
          // 一个LTTB桶里有rps个MinMax预选点（包含重复和null）
          if (times.get(j) != null) {
            emptyBucket = false; // 只要有一个MinMax预选点不是null，这个LTTB桶就不是空桶
            break;
          }
        }
        if (emptyBucket) {
          continue; // 继续往右边找非空桶
        }

        // 现在计算右边非空LTTB桶的平均点
        double rt = 0;
        double rv = 0;
        int cnt = 0;
        for (int j = nextBucket * rps; j < (nextBucket + 1) * rps; j++) {
          // 一个LTTB桶里有rps个MinMax预选点（包含重复和null）
          if (times.get(j) != null) {
            rt += times.get(j);
            rv += (double) values.get(j); // TODO
            cnt++;
          }
        }
        rt = rt / cnt;
        rv = rv / cnt;

        // 现在找到当前非空桶里距离lr垂直距离最远的点
        double maxArea = -1;
        long select_t = -1;
        double select_v = -1;
        for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
          // 一个LTTB桶里有rps个MinMax预选点（包含重复和null）
          if (times.get(j) != null) {
            long t = times.get(j);
            double v = (double) values.get(j); // TODO
            double area = IOMonitor2.calculateTri(lt, lv, t, v, rt, rv);
            System.out.printf("curr=%d,t=%d,area=%f,lt=%d%n", currentBucket, t, area, lt);
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

        // 现在更新当前桶和左边固定点，并且把结果点加到series里
        currentBucket = nextBucket;
        lt = select_t;
        lv = select_v;
      }

      // 下面处理最后一个桶
      // 现在找到当前非空桶里距离lr垂直距离最远的点
      double maxArea = -1;
      long select_t = -1;
      double select_v = -1;
      for (int j = currentBucket * rps; j < (currentBucket + 1) * rps; j++) {
        // 一个LTTB桶里有rps个MinMax预选点（包含重复和null）
        if (times.get(j) != null) {
          long t = times.get(j);
          double v = (double) values.get(j); // TODO
          double area = IOMonitor2.calculateTri(lt, lv, t, v, pnt, pnv); // 全局尾点作为右边固定点
          System.out.printf("curr=%d,t=%d,area=%f,lt=%d%n", currentBucket, t, area, lt);
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

      // 全局尾点
      series.append(pnv).append("[").append(pnt).append("]").append(",");

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
          localCurStartTime + interval < endTime; // + interval to make the last bucket complete
          // e.g, T=11,nout=3,interval=floor(11/3)=3,
          // [0,3),[3,6),[6,9), no need incomplete [9,11)
          // then the number of buckets must be Math.floor((endTime-startTime)/interval)
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
    if (CONFIG.getEnableTri().equals("MinMax") || CONFIG.getEnableTri().equals("MinMaxLTTB")) {
      return new LocalGroupByExecutorTri_MinMax(
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
