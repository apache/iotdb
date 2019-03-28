/**
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

package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.OpenedFilePathsManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.executor.groupby.GroupByWithOnlyTimeFilterDataSetDataSet;
import org.apache.iotdb.db.query.executor.groupby.GroupByWithValueFilterDataSetDataSet;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class EngineQueryRouter {

  /**
   * Each unique jdbc request(query, aggregation or others job) has an unique job id. This job id
   * will always be maintained until the request is closed. In each job, the unique file will be
   * only opened once to avoid too many opened files error.
   */
  private AtomicLong jobIdGenerator = new AtomicLong();

  /**
   * execute physical plan.
   */
  public QueryDataSet query(QueryExpression queryExpression)
      throws FileNodeManagerException {

    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);

    QueryContext context = new QueryContext();

    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression optimizedExpression = ExpressionOptimizer.getInstance()
            .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);

        if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
          EngineExecutorWithoutTimeGenerator engineExecutor =
              new EngineExecutorWithoutTimeGenerator(
                  nextJobId, queryExpression);
          return engineExecutor.executeWithGlobalTimeFilter(context);
        } else {
          EngineExecutorWithTimeGenerator engineExecutor = new EngineExecutorWithTimeGenerator(
              nextJobId,
              queryExpression);
          return engineExecutor.execute(context);
        }

      } catch (QueryFilterOptimizationException e) {
        throw new FileNodeManagerException(e);
      }
    } else {
      EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(
          nextJobId,
          queryExpression);
      return engineExecutor.executeWithoutFilter(context);
    }
  }

  /**
   * execute aggregation query.
   */
  public QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression) throws QueryFilterOptimizationException, FileNodeManagerException,
      IOException, PathErrorException, ProcessorException {

    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);

    QueryContext context = new QueryContext();

    if (expression != null) {
      IExpression optimizedExpression = ExpressionOptimizer.getInstance()
          .optimize(expression, selectedSeries);
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(nextJobId,
          selectedSeries, aggres, optimizedExpression);
      if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
        return engineExecutor.executeWithOutTimeGenerator(context);
      } else {
        return engineExecutor.executeWithTimeGenerator(context);
      }
    } else {
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(nextJobId,
          selectedSeries, aggres, null);
      return engineExecutor.executeWithOutTimeGenerator(context);
    }
  }

  /**
   * execute groupBy query.
   *
   * @param selectedSeries select path list
   * @param aggres aggregation name list
   * @param expression filter expression
   * @param unit time granularity for interval partitioning, unit is ms.
   * @param origin the datum time point for interval division is divided into a time interval for
   * each TimeUnit time from this point forward and backward.
   * @param intervals time intervals, closed interval.
   */
  public QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals)
      throws ProcessorException, QueryFilterOptimizationException, FileNodeManagerException,
      PathErrorException, IOException {

    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    QueryContext context = new QueryContext();

    // check the legitimacy of intervals
    for (Pair<Long, Long> pair : intervals) {
      if (!(pair.left > 0 && pair.right > 0)) {
        throw new ProcessorException(
            String.format("Time interval<%d, %d> must be greater than 0.", pair.left, pair.right));
      }
      if (pair.right < pair.left) {
        throw new ProcessorException(String.format(
            "Interval starting time must be greater than the interval ending time, "
                + "found error interval<%d, %d>", pair.left, pair.right));
      }
    }
    // merge intervals
    List<Pair<Long, Long>> mergedIntervalList = mergeInterval(intervals);

    // construct groupBy intervals filter
    BinaryExpression intervalFilter = null;
    for (Pair<Long, Long> pair : mergedIntervalList) {
      BinaryExpression pairFilter = BinaryExpression
          .and(new GlobalTimeExpression(TimeFilter.gtEq(pair.left)),
              new GlobalTimeExpression(TimeFilter.ltEq(pair.right)));
      if (intervalFilter != null) {
        intervalFilter = BinaryExpression.or(intervalFilter, pairFilter);
      } else {
        intervalFilter = pairFilter;
      }
    }

    // merge interval filter and filtering conditions after where statements
    if (expression == null) {
      expression = intervalFilter;
    } else {
      expression = BinaryExpression.and(expression, intervalFilter);
    }

    IExpression optimizedExpression = ExpressionOptimizer.getInstance()
        .optimize(expression, selectedSeries);
    if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      GroupByWithOnlyTimeFilterDataSetDataSet groupByEngine = new GroupByWithOnlyTimeFilterDataSetDataSet(
          nextJobId, selectedSeries, unit, origin, mergedIntervalList);
      groupByEngine.initGroupBy(context, aggres, optimizedExpression);
      return groupByEngine;
    } else {
      GroupByWithValueFilterDataSetDataSet groupByEngine = new GroupByWithValueFilterDataSetDataSet(
          nextJobId,
          selectedSeries, unit, origin, mergedIntervalList);
      groupByEngine.initGroupBy(context, aggres, optimizedExpression);
      return groupByEngine;
    }
  }

  /**
   * execute fill query.
   *
   * @param fillPaths select path list
   * @param queryTime timestamp
   * @param fillType type IFill map
   */
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType)
      throws FileNodeManagerException, PathErrorException, IOException {
    long nextJobId = getNextJobId();
    QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);
    OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(nextJobId);

    QueryContext context = new QueryContext();
    FillEngineExecutor fillEngineExecutor = new FillEngineExecutor(nextJobId, fillPaths, queryTime,
        fillType);
    return fillEngineExecutor.execute(context);
  }

  /**
   * sort intervals by start time and merge overlapping intervals.
   *
   * @param intervals time interval
   */
  private List<Pair<Long, Long>> mergeInterval(List<Pair<Long, Long>> intervals) {
    // sort by interval start time.
    intervals.sort(((o1, o2) -> (int) (o1.left - o2.left)));

    LinkedList<Pair<Long, Long>> merged = new LinkedList<>();
    for (Pair<Long, Long> interval : intervals) {
      // if the list of merged intervals is empty or
      // if the current interval does not overlap with the previous, simply append it.
      if (merged.isEmpty() || merged.getLast().right < interval.left) {
        merged.add(interval);
      } else {
        // otherwise, there is overlap, so we merge the current and previous intervals.
        merged.getLast().right = Math.max(merged.getLast().right, interval.right);
      }
    }
    return merged;
  }

  private synchronized long getNextJobId() {
    return jobIdGenerator.incrementAndGet();
  }
}
