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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByLevelDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.EmptyDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class QueryRouter implements IQueryRouter {

  private static Logger logger = LoggerFactory.getLogger(QueryRouter.class);

  @Override
  public QueryDataSet rawDataQuery(RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    IExpression expression = queryPlan.getExpression();
    List<PartialPath> deduplicatedPaths = queryPlan.getDeduplicatedPaths();

    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(expression, new ArrayList<>(deduplicatedPaths));
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    queryPlan.setExpression(optimizedExpression);

    // group the vector partial paths for raw query after optimize the expression
    // because path in expressions should not be grouped
    queryPlan.transformToVector();
    RawDataQueryExecutor rawDataQueryExecutor = getRawDataQueryExecutor(queryPlan);

    if (!queryPlan.isAlignByTime()) {
      return rawDataQueryExecutor.executeNonAlign(context);
    }

    if (optimizedExpression != null
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      return rawDataQueryExecutor.executeWithValueFilter(context);
    } else if (optimizedExpression != null
        && optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      Filter timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
      TimeValuePairUtils.Intervals intervals = TimeValuePairUtils.extractTimeInterval(timeFilter);
      if (intervals.isEmpty()) {
        logger.warn("The interval of the filter {} is empty.", timeFilter);
        return new EmptyDataSet();
      }
    }
    return rawDataQueryExecutor.executeWithoutValueFilter(context);
  }

  protected RawDataQueryExecutor getRawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    return new RawDataQueryExecutor(queryPlan);
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan aggregationPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "paths:"
              + aggregationPlan.getPaths()
              + " level:"
              + Arrays.toString(aggregationPlan.getLevels())
              + " duplicatePaths:"
              + aggregationPlan.getDeduplicatedPaths()
              + " deduplicatePaths:"
              + aggregationPlan.getDeduplicatedAggregations());
    }

    IExpression expression = aggregationPlan.getExpression();
    List<PartialPath> deduplicatedPaths = aggregationPlan.getDeduplicatedPaths();

    // optimize expression to an executable one
    IExpression optimizedExpression =
        expression == null
            ? null
            : ExpressionOptimizer.getInstance()
                .optimize(expression, new ArrayList<>(deduplicatedPaths));

    aggregationPlan.setExpression(optimizedExpression);

    AggregationExecutor engineExecutor = getAggregationExecutor(context, aggregationPlan);

    QueryDataSet dataSet;

    if (optimizedExpression != null
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      dataSet = engineExecutor.executeWithValueFilter(aggregationPlan);
    } else {
      dataSet = engineExecutor.executeWithoutValueFilter(aggregationPlan);
    }

    return dataSet;
  }

  @Override
  public QueryDataSet udafQuery(UDAFPlan udafPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, IOException,
          QueryProcessException {
    if (logger.isDebugEnabled()) {
      logger.debug("paths:" + udafPlan.getPaths());
    }
    AggregationPlan innerPlan = udafPlan.getInnerAggregationPlan();
    // Infer aggregation data types for UDF input
    List<TSDataType> aggregationResultTypes = new ArrayList<>();
    for (int i = 0; i < innerPlan.getDeduplicatedPaths().size(); i++) {
      aggregationResultTypes.add(
          TypeInferenceUtils.getAggrDataType(
              innerPlan.getDeduplicatedAggregations().get(i),
              innerPlan.getDeduplicatedDataTypes().get(i)));
    }
    QueryDataSet innerQueryDataSet;
    boolean keepNull = false;
    if (innerPlan instanceof GroupByTimePlan) {
      innerQueryDataSet = groupBy((GroupByTimePlan) innerPlan, context);
      keepNull = true;
    } else {
      innerQueryDataSet = aggregate(innerPlan, context);
    }

    UDFQueryExecutor udfQueryExecutor = new UDFQueryExecutor(udafPlan);
    return udfQueryExecutor.executeFromAlignedDataSet(
        context, innerQueryDataSet, aggregationResultTypes, keepNull);
  }

  protected AggregationExecutor getAggregationExecutor(
      QueryContext context, AggregationPlan aggregationPlan) {
    return new AggregationExecutor(context, aggregationPlan);
  }

  private IExpression getOptimizeExpression(GroupByTimePlan groupByTimePlan)
      throws QueryFilterOptimizationException, QueryProcessException {
    IExpression expression = groupByTimePlan.getExpression();
    List<PartialPath> selectedSeries = groupByTimePlan.getDeduplicatedPaths();
    GlobalTimeExpression timeExpression = getTimeExpression(groupByTimePlan);

    if (expression == null) {
      expression = timeExpression;
    } else {
      expression = BinaryExpression.and(expression, timeExpression);
    }

    // optimize expression to an executable one
    return ExpressionOptimizer.getInstance().optimize(expression, new ArrayList<>(selectedSeries));
  }

  @Override
  public QueryDataSet groupBy(GroupByTimePlan groupByTimePlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "paths:"
              + groupByTimePlan.getPaths()
              + " level:"
              + Arrays.toString(groupByTimePlan.getLevels()));
    }

    GroupByEngineDataSet dataSet;
    IExpression optimizedExpression = getOptimizeExpression(groupByTimePlan);
    groupByTimePlan.setExpression(optimizedExpression);

    if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      dataSet = getGroupByWithoutValueFilterDataSet(context, groupByTimePlan);
      ((GroupByWithoutValueFilterDataSet) dataSet).initGroupBy(context, groupByTimePlan);
    } else {
      dataSet = getGroupByWithValueFilterDataSet(context, groupByTimePlan);
      ((GroupByWithValueFilterDataSet) dataSet).initGroupBy(context, groupByTimePlan);
    }

    // we support group by level for count operation
    // details at https://issues.apache.org/jira/browse/IOTDB-622
    // and UserGuide/Operation Manual/DML
    if (groupByTimePlan.isGroupByLevel()) {
      return new GroupByLevelDataSet(groupByTimePlan, dataSet);
    }
    return dataSet;
  }

  private GlobalTimeExpression getTimeExpression(GroupByTimePlan plan)
      throws QueryProcessException {
    if (plan.isSlidingStepByMonth() || plan.isIntervalByMonth()) {
      if (!plan.isAscending()) {
        throw new QueryProcessException("Group by month doesn't support order by time desc now.");
      }
      return new GlobalTimeExpression(
          (new GroupByMonthFilter(
              plan.getInterval(),
              plan.getSlidingStep(),
              plan.getStartTime(),
              plan.getEndTime(),
              plan.isSlidingStepByMonth(),
              plan.isIntervalByMonth(),
              SessionManager.getInstance().getCurrSessionTimeZone())));
    } else {
      return new GlobalTimeExpression(
          new GroupByFilter(
              plan.getInterval(), plan.getSlidingStep(), plan.getStartTime(), plan.getEndTime()));
    }
  }

  protected GroupByWithoutValueFilterDataSet getGroupByWithoutValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan) {
    return new GroupByWithoutValueFilterDataSet(context, plan);
  }

  protected GroupByWithValueFilterDataSet getGroupByWithValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan) {
    return new GroupByWithValueFilterDataSet(context, plan);
  }

  @Override
  public QueryDataSet fill(FillQueryPlan fillQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    FillQueryExecutor fillQueryExecutor = getFillExecutor(fillQueryPlan);
    return fillQueryExecutor.execute(context);
  }

  protected FillQueryExecutor getFillExecutor(FillQueryPlan plan) {
    return new FillQueryExecutor(plan);
  }

  @Override
  public QueryDataSet groupByFill(GroupByTimeFillPlan groupByFillPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException {

    GroupByFillDataSet dataSet = new GroupByFillDataSet(context, groupByFillPlan);

    groupByFillPlan.initFillRange();
    IExpression optimizedExpression = getOptimizeExpression(groupByFillPlan);
    groupByFillPlan.setExpression(optimizedExpression);

    GroupByEngineDataSet engineDataSet;
    if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      engineDataSet = getGroupByWithoutValueFilterDataSet(context, groupByFillPlan);
      ((GroupByWithoutValueFilterDataSet) engineDataSet).initGroupBy(context, groupByFillPlan);
    } else {
      engineDataSet = getGroupByWithValueFilterDataSet(context, groupByFillPlan);
      ((GroupByWithValueFilterDataSet) engineDataSet).initGroupBy(context, groupByFillPlan);
    }

    dataSet.setDataSet(engineDataSet);
    dataSet.initCache();

    return dataSet;
  }

  @Override
  public QueryDataSet lastQuery(LastQueryPlan lastQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    LastQueryExecutor lastQueryExecutor = getLastQueryExecutor(lastQueryPlan);
    return lastQueryExecutor.execute(context, lastQueryPlan);
  }

  protected LastQueryExecutor getLastQueryExecutor(LastQueryPlan lastQueryPlan) {
    return new LastQueryExecutor(lastQueryPlan);
  }

  @Override
  public QueryDataSet udtfQuery(UDTFPlan udtfPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    IExpression expression = udtfPlan.getExpression();
    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(expression, new ArrayList<>(udtfPlan.getDeduplicatedPaths()));
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    udtfPlan.setExpression(optimizedExpression);

    boolean withValueFilter =
        optimizedExpression != null && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME;
    UDFQueryExecutor udtfQueryExecutor = new UDFQueryExecutor(udtfPlan);

    if (udtfPlan.isAlignByTime()) {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterAlignByTime(context)
          : udtfQueryExecutor.executeWithoutValueFilterAlignByTime(context);
    } else {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterNonAlign(context)
          : udtfQueryExecutor.executeWithoutValueFilterNonAlign(context);
    }
  }
}
