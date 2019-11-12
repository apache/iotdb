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

package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
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
public class EngineQueryRouter implements IEngineQueryRouter {

  @Override
  public QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws StorageEngineException {

    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression optimizedExpression = ExpressionOptimizer.getInstance()
            .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);
        EngineExecutor engineExecutor =
            new EngineExecutor(queryExpression);
        if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
          return engineExecutor.executeWithoutValueFilter(context);
        } else {
          return engineExecutor.executeWithValueFilter(context);
        }

      } catch (QueryFilterOptimizationException | IOException e) {
        throw new StorageEngineException(e.getMessage());
      }
    } else {
      EngineExecutor engineExecutor = new EngineExecutor(
          queryExpression);
      try {
        return engineExecutor.executeWithoutValueFilter(context);
      } catch (IOException e) {
        throw new StorageEngineException(e.getMessage());
      }
    }
  }

  @Override
  public QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context) throws QueryFilterOptimizationException,
      StorageEngineException, QueryProcessException, IOException {
    if (expression != null) {
      IExpression optimizedExpression = ExpressionOptimizer.getInstance()
          .optimize(expression, selectedSeries);
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(
          selectedSeries, aggres, optimizedExpression);
      if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
        return engineExecutor.executeWithoutValueFilter(context);
      } else {
        return engineExecutor.executeWithValueFilter(context);
      }
    } else {
      AggregateEngineExecutor engineExecutor = new AggregateEngineExecutor(
          selectedSeries, aggres, null);
      return engineExecutor.executeWithoutValueFilter(context);
    }
  }

  @Override
  public QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context) throws QueryFilterOptimizationException, StorageEngineException,
      QueryProcessException, IOException {

    long nextJobId = context.getJobId();

    // check the legitimacy of intervals
    for (Pair<Long, Long> pair : intervals) {
//      if (!(pair.left > 0 && pair.right > 0)) {
//        throw new ProcessorException(
//            String.format("Time interval<%d, %d> must be greater than 0.", pair.left, pair.right));
//      }
      if (pair.right < pair.left) {
        throw new QueryProcessException(String.format(
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
      GroupByWithoutValueFilterDataSet groupByEngine = new GroupByWithoutValueFilterDataSet(
          nextJobId, selectedSeries, unit, origin, mergedIntervalList);
      groupByEngine.initGroupBy(context, aggres, optimizedExpression);
      return groupByEngine;
    } else {
      GroupByWithValueFilterDataSet groupByEngine = new GroupByWithValueFilterDataSet(
          nextJobId,
          selectedSeries, unit, origin, mergedIntervalList);
      groupByEngine.initGroupBy(context, aggres, optimizedExpression);
      return groupByEngine;
    }
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {

    long nextJobId = context.getJobId();

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


}
