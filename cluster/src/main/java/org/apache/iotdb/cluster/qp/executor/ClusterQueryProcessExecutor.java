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
package org.apache.iotdb.cluster.qp.executor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.executor.ClusterQueryRouter;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterQueryProcessExecutor extends AbstractQPExecutor implements IQueryProcessExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQueryProcessExecutor.class);
  private ThreadLocal<Integer> fetchSize = new ThreadLocal<>();
  private ClusterQueryRouter clusterQueryRouter = new ClusterQueryRouter();

  private QueryMetadataExecutor queryMetadataExecutor;

  public ClusterQueryProcessExecutor(
      QueryMetadataExecutor queryMetadataExecutor) {
    this.queryMetadataExecutor = queryMetadataExecutor;
  }

  @Override
  public QueryDataSet processQuery(QueryPlan queryPlan, QueryContext context)
      throws IOException, FileNodeManagerException, PathErrorException,
      QueryFilterOptimizationException, ProcessorException {

    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    clusterQueryRouter.setReadDataConsistencyLevel(getReadDataConsistencyLevel());
    if (queryPlan instanceof GroupByPlan) {
      GroupByPlan groupByPlan = (GroupByPlan) queryPlan;
      return groupBy(groupByPlan.getPaths(), groupByPlan.getAggregations(),
          groupByPlan.getExpression(), groupByPlan.getUnit(), groupByPlan.getOrigin(),
          groupByPlan.getIntervals(), context);
    }

    if (queryPlan instanceof AggregationPlan) {
      return aggregate(queryPlan.getPaths(), queryPlan.getAggregations(),
          queryPlan.getExpression(), context);
    }

    if (queryPlan instanceof FillQueryPlan) {
      FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
      return fill(queryPlan.getPaths(), fillQueryPlan.getQueryTime(),
          fillQueryPlan.getFillType(), context);
    }
    return clusterQueryRouter.query(queryExpression, context);
  }

  @Override
  public QueryDataSet aggregate(List<Path> paths, List<String> aggres, IExpression expression,
      QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException, QueryFilterOptimizationException {
    return clusterQueryRouter.aggregate(paths, aggres, expression, context);
  }

  @Override
  public QueryDataSet groupBy(List<Path> paths, List<String> aggres, IExpression expression,
      long unit, long origin, List<Pair<Long, Long>> intervals, QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException, QueryFilterOptimizationException {
    return clusterQueryRouter.groupBy(paths, aggres, expression, unit, origin, intervals, context);
  }

  @Override
  public QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType, IFill> fillTypes,
      QueryContext context)
      throws ProcessorException, IOException, PathErrorException, FileNodeManagerException {
    return clusterQueryRouter.fill(fillPaths, queryTime, fillTypes, context);
  }

  @Override
  public TSDataType getSeriesType(Path path) throws PathErrorException {
    if (path.equals(SQLConstant.RESERVED_TIME)) {
      return TSDataType.INT64;
    }
    if (path.equals(SQLConstant.RESERVED_FREQ)) {
      return TSDataType.FLOAT;
    }
    try {
      return queryMetadataExecutor.processSeriesTypeQuery(path.getFullPath());
    } catch (InterruptedException | ProcessorException e) {
      throw new PathErrorException(e.getMessage());
    }
  }

  @Override
  public List<String> getAllPaths(String originPath)
      throws PathErrorException {
    try {
      LOGGER.debug("read metadata level :" + getReadMetadataConsistencyLevel());
      return queryMetadataExecutor.processPathsQuery(originPath);
    } catch (InterruptedException | ProcessorException e) {
      throw new PathErrorException(e.getMessage());
    }
  }

  @Override
  public boolean judgePathExists(Path fullPath) {
    try {
      List<List<String>> results = queryMetadataExecutor.processTimeSeriesQuery(fullPath.toString());
      return !results.isEmpty();
    } catch (InterruptedException | PathErrorException | ProcessorException e) {
      return false;
    }
  }

  @Override
  public int getFetchSize() {
    return fetchSize.get();
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize.set(fetchSize);
  }

  @Override
  public boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(List<Path> paths, long deleteTime) throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path path, long deleteTime) throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int insert(Path path, long insertTime, String value) throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int multiInsert(String deviceId, long insertTime, String[] measurementList,
      String[] insertValues) throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
    throw new UnsupportedOperationException();
  }
}
