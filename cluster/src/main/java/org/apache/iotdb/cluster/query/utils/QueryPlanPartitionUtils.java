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
package org.apache.iotdb.cluster.query.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterGroupEntity;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;

/**
 * Utils for splitting query plan to several sub query plans by group id.
 */
public class QueryPlanPartitionUtils {

  private QueryPlanPartitionUtils() {

  }

  /**
   * Split query plan with no filter or with only global time filter by group id
   */
  public static void splitQueryPlanWithoutValueFilter(
      ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    splitQueryPlanBySelectPath(singleQueryManager);
  }

  /**
   * Split query plan by select paths
   */
  private static void splitQueryPlanBySelectPath(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getOriginQueryPlan();
    Map<String, List<Path>> selectSeriesByGroupId = singleQueryManager.getSelectSeriesByGroupId();
    Map<String, QueryPlan> selectPathPlans = singleQueryManager.getSelectPathPlans();
    List<Path> selectPaths = queryPlan.getPaths();
    for (Path path : selectPaths) {
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (!selectSeriesByGroupId.containsKey(groupId)) {
        selectSeriesByGroupId.put(groupId, new ArrayList<>());
      }
      selectSeriesByGroupId.get(groupId).add(path);
    }
    for (Entry<String, List<Path>> entry : selectSeriesByGroupId.entrySet()) {
      String groupId = entry.getKey();
      List<Path> paths = entry.getValue();
      QueryPlan subQueryPlan = new QueryPlan();
      subQueryPlan.setProposer(queryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      subQueryPlan.setExpression(queryPlan.getExpression());
      selectPathPlans.put(groupId, subQueryPlan);
    }
  }

  /**
   * Split query plan with filter.
   */
  public static void splitQueryPlanWithValueFilter(
      ClusterRpcSingleQueryManager singleQueryManager) throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getOriginQueryPlan();
    if (queryPlan instanceof GroupByPlan) {
      splitGroupByPlan((GroupByPlan) queryPlan, singleQueryManager);
    } else if (queryPlan instanceof AggregationPlan) {
      splitAggregationPlan((AggregationPlan) queryPlan, singleQueryManager);
    } else {
      splitQueryPlan(queryPlan, singleQueryManager);
    }
  }

  private static void splitGroupByPlan(GroupByPlan queryPlan,
      ClusterRpcSingleQueryManager singleQueryManager) {
    throw new UnsupportedOperationException();
  }

  private static void splitAggregationPlan(AggregationPlan aggregationPlan,
      ClusterRpcSingleQueryManager singleQueryManager) {
    throw new UnsupportedOperationException();
  }

  private static void splitQueryPlan(QueryPlan queryPlan,
      ClusterRpcSingleQueryManager singleQueryManager) throws PathErrorException {
    splitQueryPlanBySelectPath(singleQueryManager);
    // split query plan by filter path
    Map<String, FilterGroupEntity> filterGroupEntityMap = singleQueryManager
        .getFilterGroupEntityMap();
    IExpression expression = queryPlan.getExpression();
    ExpressionUtils.getAllExpressionSeries(expression, filterGroupEntityMap);
    for (FilterGroupEntity filterGroupEntity : filterGroupEntityMap.values()) {
      List<Path> filterSeriesList = filterGroupEntity.getFilterPaths();
      // create filter sub query plan
      QueryPlan subQueryPlan = new QueryPlan();
      subQueryPlan.setPaths(filterSeriesList);
      IExpression subExpression = ExpressionUtils
          .pruneFilterTree(expression.clone(), filterSeriesList);
      if (subExpression.getType() != ExpressionType.TRUE) {
        subQueryPlan.setExpression(subExpression);
      }
      filterGroupEntity.setQueryPlan(subQueryPlan);
    }
  }
}
