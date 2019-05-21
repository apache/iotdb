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
package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.QueryType;
import org.apache.iotdb.cluster.query.dataset.ClusterGroupByDataSetWithOnlyTimeFilter;
import org.apache.iotdb.cluster.query.dataset.ClusterGroupByDataSetWithTimeGenerator;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AbstractQueryRouter;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
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
 * Query entrance class of cluster query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by ClusterQueryRouter.
 */
public class ClusterQueryRouter extends AbstractQueryRouter {

  /**
   * Consistency level of reading data
   */
  private ThreadLocal<Integer> readDataConsistencyLevel = new ThreadLocal<>();

  @Override
  public QueryDataSet query(QueryExpression queryExpression, QueryContext context)
      throws FileNodeManagerException, PathErrorException {

    ClusterRpcSingleQueryManager queryManager = ClusterRpcQueryManager.getInstance()
        .getSingleQuery(context.getJobId());
    try {
      if (queryExpression.hasQueryFilter()) {

        IExpression optimizedExpression = ExpressionOptimizer.getInstance()
            .optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);
        // update query expression of origin query plan, it's necessary.
        queryManager.getOriginQueryPlan().setExpression(optimizedExpression);

        if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
          queryManager.initQueryResource(QueryType.GLOBAL_TIME, getReadDataConsistencyLevel());
          ClusterExecutorWithoutTimeGenerator engineExecutor =
              new ClusterExecutorWithoutTimeGenerator(queryExpression, queryManager);
          return engineExecutor.execute(context);
        } else {
          queryManager.initQueryResource(QueryType.FILTER, getReadDataConsistencyLevel());
          ClusterExecutorWithTimeGenerator engineExecutor = new ClusterExecutorWithTimeGenerator(
              queryExpression, queryManager);
          return engineExecutor.execute(context);
        }

      } else {
        queryManager.initQueryResource(QueryType.NO_FILTER, getReadDataConsistencyLevel());
        ClusterExecutorWithoutTimeGenerator engineExecutor =
            new ClusterExecutorWithoutTimeGenerator(queryExpression, queryManager);
        return engineExecutor.execute(context);
      }
    } catch (QueryFilterOptimizationException | IOException | RaftConnectionException e) {
      throw new FileNodeManagerException(e);
    }
  }

  @Override
  public QueryDataSet aggregate(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, QueryContext context)
      throws FileNodeManagerException, PathErrorException, ProcessorException {

    ClusterRpcSingleQueryManager queryManager = ClusterRpcQueryManager.getInstance()
        .getSingleQuery(context.getJobId());

    try {
      if (expression != null) {
        IExpression optimizedExpression = ExpressionOptimizer.getInstance()
            .optimize(expression, selectedSeries);
        AggregateEngineExecutor engineExecutor = new ClusterAggregateEngineExecutor(
            selectedSeries, aggres, optimizedExpression, queryManager);
        if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
          queryManager.initQueryResource(QueryType.GLOBAL_TIME, getReadDataConsistencyLevel());
          return engineExecutor.executeWithoutTimeGenerator(context);
        } else {
          queryManager.initQueryResource(QueryType.FILTER, getReadDataConsistencyLevel());
          return engineExecutor.executeWithTimeGenerator(context);
        }
      } else {
        AggregateEngineExecutor engineExecutor = new ClusterAggregateEngineExecutor(
            selectedSeries, aggres, null, queryManager);
        queryManager.initQueryResource(QueryType.NO_FILTER, getReadDataConsistencyLevel());
        return engineExecutor.executeWithoutTimeGenerator(context);
      }
    } catch (QueryFilterOptimizationException | IOException | RaftConnectionException e) {
      throw new FileNodeManagerException(e);
    }
  }

  @Override
  public QueryDataSet groupBy(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context)
      throws ProcessorException, QueryFilterOptimizationException, FileNodeManagerException, PathErrorException, IOException {

    long jobId = context.getJobId();
    ClusterRpcSingleQueryManager queryManager = ClusterRpcQueryManager.getInstance()
        .getSingleQuery(jobId);

    //check the legitimacy of intervals
    checkIntervals(intervals);

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
    try {
      if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
        ClusterGroupByDataSetWithOnlyTimeFilter groupByEngine = new ClusterGroupByDataSetWithOnlyTimeFilter(
            jobId, selectedSeries, unit, origin, mergedIntervalList, queryManager);
        groupByEngine.initGroupBy(context, aggres, optimizedExpression);
        return groupByEngine;
      } else {
        queryManager.initQueryResource(QueryType.FILTER, getReadDataConsistencyLevel());
        ClusterGroupByDataSetWithTimeGenerator groupByEngine = new ClusterGroupByDataSetWithTimeGenerator(
            jobId, selectedSeries, unit, origin, mergedIntervalList, queryManager);
        groupByEngine.initGroupBy(context, aggres, optimizedExpression);
        return groupByEngine;
      }
    } catch (RaftConnectionException e) {
      throw new FileNodeManagerException(e);
    }
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillType,
      QueryContext context) throws FileNodeManagerException, PathErrorException, IOException {
    ClusterRpcSingleQueryManager queryManager = ClusterRpcQueryManager.getInstance()
        .getSingleQuery(context.getJobId());
    try {
      queryManager.initQueryResource(QueryType.NO_FILTER, getReadDataConsistencyLevel());

      ClusterFillEngineExecutor fillEngineExecutor = new ClusterFillEngineExecutor(fillPaths,
          queryTime,
          fillType, queryManager);
      return fillEngineExecutor.execute(context);
    } catch (IOException | RaftConnectionException e) {
      throw new FileNodeManagerException(e);
    }
  }

  public int getReadDataConsistencyLevel() {
    return readDataConsistencyLevel.get();
  }

  public void setReadDataConsistencyLevel(int readDataConsistencyLevel) {
    this.readDataConsistencyLevel.set(readDataConsistencyLevel);
  }
}
