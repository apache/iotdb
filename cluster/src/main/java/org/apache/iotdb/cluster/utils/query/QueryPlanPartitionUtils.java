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
package org.apache.iotdb.cluster.utils.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Utils for spliting query plan to several sub query plan by group id.
 */
public class QueryPlanPartitionUtils {

  private QueryPlanPartitionUtils() {

  }

  /**
   * Split query plan with no filter or with only global time filter by group id
   */
  public static void splitQueryPlanWithoutValueFilter(ClusterRpcSingleQueryManager singleQueryManager)
      throws PathErrorException {
    QueryPlan queryPlan = singleQueryManager.getQueryPlan();
    Map<String, List<String>> selectSeriesByGroupId = singleQueryManager.getSelectSeriesByGroupId();
    Map<String, QueryPlan> selectPathPlans = singleQueryManager.getSelectPathPlans();
    List<Path> selectPaths = queryPlan.getPaths();
    Map<String, List<Path>> selectPathsByGroupId = new HashMap<>();
    for (Path path : selectPaths) {
      String storageGroup = QPExecutorUtils.getStroageGroupByDevice(path.getDevice());
      String groupId = Router.getInstance().getGroupIdBySG(storageGroup);
      if (selectPathsByGroupId.containsKey(groupId)) {
        selectPathsByGroupId.put(groupId, new ArrayList<>());
        selectSeriesByGroupId.put(groupId, new ArrayList<>());
      }
      selectPathsByGroupId.get(groupId).add(path);
      selectSeriesByGroupId.get(groupId).add(path.getFullPath());
    }
    for (Entry<String, List<Path>> entry : selectPathsByGroupId.entrySet()) {
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
   * Split query plan with not only global time filter.
   */
  public static void splitQueryPlanWithValueFilter(ClusterRpcSingleQueryManager singleQueryManager) {

  }
}
