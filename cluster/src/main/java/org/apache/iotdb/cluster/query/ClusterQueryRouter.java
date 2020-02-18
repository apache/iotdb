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

import java.io.IOException;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterQueryRouter extends EngineQueryRouter {

  private MetaGroupMember metaGroupMember;

  ClusterQueryRouter(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
    executorFactory = (paths, types, expr) -> new ClusterDataQueryExecutor(paths, types, expr,
        this.metaGroupMember);
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan plan, QueryContext context) {
    throw new UnsupportedOperationException("Aggregate not implemented");
  }

  @Override
  public QueryDataSet fill(FillQueryPlan plan, QueryContext context) {
    throw new UnsupportedOperationException("Fill not implemented");
  }

  @Override
  public QueryDataSet groupBy(GroupByPlan groupByPlan, QueryContext context) {
    throw new UnsupportedOperationException("GroupBy not implemented");
  }

  @Override
  protected AggregateEngineExecutor getAggregateEngine(AggregationPlan aggregationPlan) {
    return new ClusterAggregateExecutor(aggregationPlan);
  }
}
