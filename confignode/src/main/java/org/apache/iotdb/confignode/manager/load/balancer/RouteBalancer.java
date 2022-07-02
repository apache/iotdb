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
package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.IRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LeaderRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LoadScoreGreedyRouter;

import java.util.List;
import java.util.Map;

/**
 * The RouteBalancer plays the role of load information collector since different routing policy
 * need different load information.
 */
public class RouteBalancer {

  public static final String leaderPolicy = "leader";
  public static final String greedyPolicy = "greedy";

  private final IManager configManager;

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> genRealTimeRoutingPolicy(
      List<TRegionReplicaSet> regionReplicaSets) {
    return genRouter().genRealTimeRoutingPolicy(regionReplicaSets);
  }

  private IRouter genRouter() {
    String policy = ConfigNodeDescriptor.getInstance().getConf().getRoutingPolicy();
    if (policy.equals(leaderPolicy)) {
      return new LeaderRouter(
          getLoadManager().getAllLeadership(), getLoadManager().getAllLoadScores());
    } else {
      return new LoadScoreGreedyRouter(getLoadManager().getAllLoadScores());
    }
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
