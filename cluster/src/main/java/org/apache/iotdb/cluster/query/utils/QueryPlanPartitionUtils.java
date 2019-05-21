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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.manager.coordinatornode.FilterSeriesGroupEntity;
import org.apache.iotdb.cluster.query.manager.coordinatornode.SelectSeriesGroupEntity;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
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
   * Split query plan with no filter, with only global time filter by group id or fill query
   */
  public static void splitQueryPlanWithoutValueFilter(
      ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    QueryPlan queryPLan = singleQueryManager.getOriginQueryPlan();
    if (queryPLan instanceof FillQueryPlan) {
      splitFillPlan(singleQueryManager);
    } else if (queryPLan instanceof GroupByPlan) {
      splitGroupByPlanBySelectPath(singleQueryManager);
    } else if (queryPLan instanceof AggregationPlan) {
      splitAggregationPlanBySelectPath(singleQueryManager);
    } else {
      splitQueryPlanBySelectPath(singleQueryManager);
    }
  }

  /**
   * Split query plan with filter.
   */
  public static void splitQueryPlanWithValueFilter(
      ClusterRpcSingleQueryManager singleQueryManager) throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getOriginQueryPlan();
    if (queryPlan instanceof GroupByPlan) {
      splitGroupByPlanWithFilter(singleQueryManager);
    } else if (queryPlan instanceof AggregationPlan) {
      splitAggregationPlanWithFilter(singleQueryManager);
    } else {
      splitQueryPlanWithFilter(singleQueryManager);
    }
  }

  /**
   * Split query plan by select paths
   */
  private static void splitQueryPlanBySelectPath(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getOriginQueryPlan();
    // split query plan by select path
    Map<String, SelectSeriesGroupEntity> selectGroupEntityMap = singleQueryManager
        .getSelectSeriesGroupEntityMap();
    List<Path> selectPaths = queryPlan.getPaths();
    for (Path path : selectPaths) {
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (!selectGroupEntityMap.containsKey(groupId)) {
        selectGroupEntityMap.put(groupId, new SelectSeriesGroupEntity(groupId));
      }
      selectGroupEntityMap.get(groupId).addSelectPaths(path);
    }
    for (SelectSeriesGroupEntity entity : selectGroupEntityMap.values()) {
      List<Path> paths = entity.getSelectPaths();
      QueryPlan subQueryPlan = new QueryPlan();
      subQueryPlan.setProposer(queryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      subQueryPlan.setExpression(queryPlan.getExpression());
      entity.setQueryPlan(subQueryPlan);
    }
  }


  /**
   * Split query plan by filter paths
   */
  private static void splitQueryPlanByFilterPath(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getOriginQueryPlan();
    // split query plan by filter path
    Map<String, FilterSeriesGroupEntity> filterGroupEntityMap = singleQueryManager
        .getFilterSeriesGroupEntityMap();
    IExpression expression = queryPlan.getExpression();
    ExpressionUtils.getAllExpressionSeries(expression, filterGroupEntityMap);
    for (FilterSeriesGroupEntity filterSeriesGroupEntity : filterGroupEntityMap.values()) {
      List<Path> filterSeriesList = filterSeriesGroupEntity.getFilterPaths();
      // create filter sub query plan
      QueryPlan subQueryPlan = new QueryPlan();
      subQueryPlan.setPaths(filterSeriesList);
      IExpression subExpression = ExpressionUtils
          .pruneFilterTree(expression.clone(), filterSeriesList);
      if (subExpression.getType() != ExpressionType.TRUE) {
        subQueryPlan.setExpression(subExpression);
      }
      filterSeriesGroupEntity.setQueryPlan(subQueryPlan);
    }
  }

  /**
   * Split group by plan by select path
   */
  private static void splitGroupByPlanBySelectPath(
      ClusterRpcSingleQueryManager singleQueryManager) throws PathErrorException {
    GroupByPlan queryPlan = (GroupByPlan) singleQueryManager.getOriginQueryPlan();
    List<Path> selectPaths = queryPlan.getPaths();
    List<String> aggregations = queryPlan.getAggregations();
    Map<String, SelectSeriesGroupEntity> selectGroupEntityMap = singleQueryManager
        .getSelectSeriesGroupEntityMap();
    Map<String, List<String>> selectAggregationByGroupId = new HashMap<>();
    for (int i = 0; i < selectPaths.size(); i++) {
      String aggregation = aggregations.get(i);
      Path path = selectPaths.get(i);
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (!selectGroupEntityMap.containsKey(groupId)) {
        selectGroupEntityMap.put(groupId, new SelectSeriesGroupEntity(groupId));
        selectAggregationByGroupId.put(groupId, new ArrayList<>());
      }
      selectGroupEntityMap.get(groupId).addSelectPaths(path);
      selectAggregationByGroupId.get(groupId).add(aggregation);
    }
    for (Entry<String, SelectSeriesGroupEntity> entry : selectGroupEntityMap.entrySet()) {
      String groupId = entry.getKey();
      SelectSeriesGroupEntity entity = entry.getValue();
      List<Path> paths = entity.getSelectPaths();
      GroupByPlan subQueryPlan = new GroupByPlan();
      subQueryPlan.setIntervals(queryPlan.getIntervals());
      subQueryPlan.setOrigin(queryPlan.getOrigin());
      subQueryPlan.setUnit(queryPlan.getUnit());
      subQueryPlan.setProposer(queryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      subQueryPlan.setExpression(queryPlan.getExpression());
      subQueryPlan.setAggregations(selectAggregationByGroupId.get(groupId));
      entity.setQueryPlan(subQueryPlan);
    }
  }

  /**
   * Split group by plan with filter path
   */
  private static void splitGroupByPlanWithFilter(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    splitGroupByPlanBySelectPath(singleQueryManager);
    splitQueryPlanByFilterPath(singleQueryManager);
  }

  /**
   * Split aggregation plan by select path
   */
  private static void splitAggregationPlanBySelectPath(
      ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    AggregationPlan queryPlan = (AggregationPlan) singleQueryManager.getOriginQueryPlan();
    List<Path> selectPaths = queryPlan.getPaths();
    List<String> aggregations = queryPlan.getAggregations();
    Map<String, List<String>> selectAggregationByGroupId = new HashMap<>();
    Map<String, SelectSeriesGroupEntity> selectGroupEntityMap = singleQueryManager
        .getSelectSeriesGroupEntityMap();
    for (int i = 0; i < selectPaths.size(); i++) {
      Path path = selectPaths.get(i);
      String aggregation = aggregations.get(i);
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (!selectGroupEntityMap.containsKey(groupId)) {
        selectGroupEntityMap.put(groupId, new SelectSeriesGroupEntity(groupId));
        selectAggregationByGroupId.put(groupId, new ArrayList<>());
      }
      selectAggregationByGroupId.get(groupId).add(aggregation);
      selectGroupEntityMap.get(groupId).addSelectPaths(path);
    }
    for (Entry<String, SelectSeriesGroupEntity> entry : selectGroupEntityMap.entrySet()) {
      String groupId = entry.getKey();
      SelectSeriesGroupEntity entity = entry.getValue();
      List<Path> paths = entity.getSelectPaths();
      AggregationPlan subQueryPlan = new AggregationPlan();
      subQueryPlan.setProposer(queryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      subQueryPlan.setExpression(queryPlan.getExpression());
      subQueryPlan.setAggregations(selectAggregationByGroupId.get(groupId));
      entity.setQueryPlan(subQueryPlan);
    }
  }

  /**
   * Split aggregation plan with filter path
   */
  private static void splitAggregationPlanWithFilter(
      ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    splitAggregationPlanBySelectPath(singleQueryManager);
    splitQueryPlanByFilterPath(singleQueryManager);
  }

  /**
   * Split fill plan which only contain select paths.
   */
  private static void splitFillPlan(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    FillQueryPlan fillQueryPlan = (FillQueryPlan) singleQueryManager.getOriginQueryPlan();
    List<Path> selectPaths = fillQueryPlan.getPaths();
    Map<String, SelectSeriesGroupEntity> selectGroupEntityMap = singleQueryManager
        .getSelectSeriesGroupEntityMap();
    for (Path path : selectPaths) {
      String groupId = QPExecutorUtils.getGroupIdByDevice(path.getDevice());
      if (!selectGroupEntityMap.containsKey(groupId)) {
        selectGroupEntityMap.put(groupId, new SelectSeriesGroupEntity(groupId));
      }
      selectGroupEntityMap.get(groupId).addSelectPaths(path);
    }
    for (SelectSeriesGroupEntity entity : selectGroupEntityMap.values()) {
      List<Path> paths = entity.getSelectPaths();
      FillQueryPlan subQueryPlan = new FillQueryPlan();
      subQueryPlan.setProposer(fillQueryPlan.getProposer());
      subQueryPlan.setPaths(paths);
      subQueryPlan.setExpression(fillQueryPlan.getExpression());
      subQueryPlan.setQueryTime(fillQueryPlan.getQueryTime());
      subQueryPlan.setFillType(new EnumMap<>(fillQueryPlan.getFillType()));
      entity.setQueryPlan(subQueryPlan);
    }
  }

  /**
   * Split query plan with filter
   */
  private static void splitQueryPlanWithFilter(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    splitQueryPlanBySelectPath(singleQueryManager);
    splitQueryPlanByFilterPath(singleQueryManager);
  }

}
