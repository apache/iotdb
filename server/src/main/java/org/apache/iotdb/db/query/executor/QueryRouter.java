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
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByLevelDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByTimeDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByTimeEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
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
    RawDataQueryExecutor rawDataQueryExecutor = getRawDataQueryExecutor(queryPlan);

    if (!queryPlan.isAlignByTime()) {
      // group the vector partial paths for raw query after optimize the expression
      // because path in expressions should not be grouped
      queryPlan.transformToVector();
      return rawDataQueryExecutor.executeNonAlign(context);
    }

    if (queryPlan.getExpression() != null
        && queryPlan.getExpression().getType() != ExpressionType.GLOBAL_TIME) {
      return rawDataQueryExecutor.executeWithValueFilter(context);
    } else if (queryPlan.getExpression() != null
        && queryPlan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
      Filter timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
      TimeValuePairUtils.Intervals intervals = TimeValuePairUtils.extractTimeInterval(timeFilter);
      if (intervals.isEmpty()) {
        logger.warn("The interval of the filter {} is empty.", timeFilter);
        return new EmptyDataSet();
      }
    }

    // group the vector partial paths for raw query after optimize the expression
    // because path in expressions should not be grouped
    queryPlan.transformToVector();
    return rawDataQueryExecutor.executeWithoutValueFilter(context);
  }

  protected RawDataQueryExecutor getRawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    return new RawDataQueryExecutor(queryPlan);
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan aggregationPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {

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

    AggregationExecutor engineExecutor = getAggregationExecutor(context, aggregationPlan);

    QueryDataSet dataSet;

    if (aggregationPlan.getExpression() != null
        && aggregationPlan.getExpression().getType() != ExpressionType.GLOBAL_TIME) {
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
    // We set keepNull to true when we want to keep the rows whose fields are all null except the
    // timestamp field.
    boolean keepNull = false;
    if (innerPlan instanceof GroupByTimePlan) {
      innerQueryDataSet = groupBy((GroupByTimePlan) innerPlan, context);
      // In GroupByTimePlan, we think it is better to keep the windows with null values, so we set
      // keepNull to true.
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

    GroupByTimeEngineDataSet dataSet;

    if (groupByTimePlan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
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
      throws StorageEngineException, QueryProcessException {

    GroupByFillDataSet dataSet = new GroupByFillDataSet(context, groupByFillPlan);

    GroupByTimeDataSet engineDataSet;
    // reset queryStartTime and queryEndTime for init GroupByEngineDataSet
    groupByFillPlan.setQueryStartTime(groupByFillPlan.getStartTime());
    groupByFillPlan.setQueryEndTime(groupByFillPlan.getEndTime());
    if (groupByFillPlan.getExpression().getType() == ExpressionType.GLOBAL_TIME) {
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
    boolean withValueFilter =
        udtfPlan.getExpression() != null
            && udtfPlan.getExpression().getType() != ExpressionType.GLOBAL_TIME;
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
