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
package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/** The LoadScoreGreedyRouter always pick the Replica with the lowest loadScore */
public class LoadScoreGreedyRouter implements IRouter {

  // Map<DataNodeId, loadScore>
  private final Map<Integer, Float> loadScoreMap;

  /** The constructor is used to pass in the load information needed by the algorithm */
  public LoadScoreGreedyRouter(Map<Integer, Float> loadScoreMap) {
    this.loadScoreMap = loadScoreMap;
  }

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> genRealTimeRoutingPolicy(
      List<TRegionReplicaSet> replicaSets) {
    Map<TConsensusGroupId, TRegionReplicaSet> result = new ConcurrentHashMap<>();

    replicaSets.forEach(
        replicaSet -> {
          TRegionReplicaSet sortedReplicaSet = new TRegionReplicaSet();
          sortedReplicaSet.setRegionId(replicaSet.getRegionId());

          // List<Pair<loadScore, TDataNodeLocation>> for sorting
          List<Pair<Double, TDataNodeLocation>> sortList = new Vector<>();
          replicaSet
              .getDataNodeLocations()
              .forEach(
                  dataNodeLocation -> {
                    // The absenteeism of loadScoreMap means ConfigNode-leader doesn't receive any
                    // heartbeat from that DataNode.
                    // In this case we put a maximum loadScore into the sortList.
                    sortList.add(
                        new Pair<>(
                            (double)
                                loadScoreMap.computeIfAbsent(
                                    dataNodeLocation.getDataNodeId(), empty -> Float.MAX_VALUE),
                            dataNodeLocation));
                  });

          sortList.sort(Comparator.comparingDouble(Pair::getLeft));
          for (Pair<Double, TDataNodeLocation> entry : sortList) {
            sortedReplicaSet.addToDataNodeLocations(entry.getRight());
          }

          result.put(sortedReplicaSet.getRegionId(), sortedReplicaSet);
        });

    return result;
  }
}
