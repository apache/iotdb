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
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public abstract class AbstractQueryRouter {

  /**
   * Execute physical plan.
   */
  public abstract QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws FileNodeManagerException, PathErrorException;

  /**
   * Execute aggregation query.
   */
  public abstract QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context)
      throws QueryFilterOptimizationException, FileNodeManagerException, IOException, PathErrorException, ProcessorException;

  /**
   * Execute groupBy query.
   *
   * @param selectedSeries select path list
   * @param aggres aggregation name list
   * @param expression filter expression
   * @param unit time granularity for interval partitioning, unit is ms.
   * @param origin the datum time point for interval division is divided into a time interval for
   * each TimeUnit time from this point forward and backward.
   * @param intervals time intervals, closed interval.
   */
  public abstract QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context)
      throws ProcessorException, QueryFilterOptimizationException, FileNodeManagerException,
      PathErrorException, IOException;
  /**
   * Execute fill query.
   *
   * @param fillPaths select path list
   * @param queryTime timestamp
   * @param fillType type IFill map
   */
  public abstract QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) throws FileNodeManagerException, PathErrorException, IOException;


  /**
   * sort intervals by start time and merge overlapping intervals.
   *
   * @param intervals time interval
   */
  protected List<Pair<Long, Long>> mergeInterval(List<Pair<Long, Long>> intervals) {
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

  /**
   * Check the legitimacy of intervals
   */
  protected void checkIntervals(List<Pair<Long, Long>> intervals) throws ProcessorException {
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
  }

}
