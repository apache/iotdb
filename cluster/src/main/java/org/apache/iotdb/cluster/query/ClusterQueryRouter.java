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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.query.aggregate.ClusterAggregateExecutor;
import org.apache.iotdb.cluster.query.fill.ClusterFillExecutor;
import org.apache.iotdb.cluster.query.groupby.ClusterGroupByFillNoVFilterDataSet;
import org.apache.iotdb.cluster.query.groupby.ClusterGroupByFillVFilterDataSet;
import org.apache.iotdb.cluster.query.groupby.ClusterGroupByNoVFilterDataSet;
import org.apache.iotdb.cluster.query.groupby.ClusterGroupByVFilterDataSet;
import org.apache.iotdb.cluster.query.last.ClusterLastQueryExecutor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillWithoutValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.executor.FillQueryExecutor;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.query.executor.RawDataQueryExecutor;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;

public class ClusterQueryRouter extends QueryRouter {

  private MetaGroupMember metaGroupMember;

  ClusterQueryRouter(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected FillQueryExecutor getFillExecutor(FillQueryPlan plan) {
    return new ClusterFillExecutor(plan, metaGroupMember);
  }

  @Override
  protected GroupByWithoutValueFilterDataSet getGroupByWithoutValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan) {
    return new ClusterGroupByNoVFilterDataSet(context, plan, metaGroupMember);
  }

  @Override
  protected GroupByWithValueFilterDataSet getGroupByWithValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan) {
    return new ClusterGroupByVFilterDataSet(context, plan, metaGroupMember);
  }

  @Override
  protected GroupByFillWithValueFilterDataSet getGroupByFillWithValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByTimeFillPlan)
      throws QueryProcessException, StorageEngineException {
    return new ClusterGroupByFillVFilterDataSet(context, groupByTimeFillPlan, metaGroupMember);
  }

  @Override
  protected GroupByFillWithoutValueFilterDataSet getGroupByFillWithoutValueFilterDataSet(
      QueryContext context, GroupByTimeFillPlan groupByFillPlan)
      throws QueryProcessException, StorageEngineException {
    return new ClusterGroupByFillNoVFilterDataSet(context, groupByFillPlan, metaGroupMember);
  }

  @Override
  protected AggregationExecutor getAggregationExecutor(
      QueryContext context, AggregationPlan aggregationPlan) {
    return new ClusterAggregateExecutor(context, aggregationPlan, metaGroupMember);
  }

  @Override
  protected RawDataQueryExecutor getRawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    return new ClusterDataQueryExecutor(queryPlan, metaGroupMember);
  }

  @Override
  protected LastQueryExecutor getLastQueryExecutor(LastQueryPlan lastQueryPlan) {
    return new ClusterLastQueryExecutor(lastQueryPlan, metaGroupMember);
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
    ClusterUDTFQueryExecutor clusterUDTFQueryExecutor =
        new ClusterUDTFQueryExecutor(udtfPlan, metaGroupMember);

    if (udtfPlan.isAlignByTime()) {
      return withValueFilter
          ? clusterUDTFQueryExecutor.executeWithValueFilterAlignByTime(context)
          : clusterUDTFQueryExecutor.executeWithoutValueFilterAlignByTime(context);
    } else {
      return withValueFilter
          ? clusterUDTFQueryExecutor.executeWithValueFilterNonAlign(context)
          : clusterUDTFQueryExecutor.executeWithoutValueFilterNonAlign(context);
    }
  }
}
