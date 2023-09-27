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

package org.apache.iotdb.db.queryengine.execution.load.locseq;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.execution.load.locseq.LocationStatistics.Statistic;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ThroughputBasedLocationSequencer implements LocationSequencer {

  private static final Logger logger =
      LoggerFactory.getLogger(ThroughputBasedLocationSequencer.class);
  private Random random = new Random();
  private long resampleThresholdMS = 10000000;
  private List<TDataNodeLocation> orderedLocations;

  public ThroughputBasedLocationSequencer(
      TRegionReplicaSet replicaSet, LocationStatistics locationStatistics) {
    List<Pair<TDataNodeLocation, Double>> locationRanks =
        rankLocations(replicaSet, locationStatistics);
    orderedLocations = new ArrayList<>(locationRanks.size());
    while (!locationRanks.isEmpty()) {
      // the chosen location is removed from the list
      orderedLocations.add(chooseNextLocation(locationRanks).left);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Location orders: {}",
          orderedLocations.stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
    }
  }

  /**
   * The rank (probability of being chosen) is calculated as throughput / totalThroughput for those
   * nodes that have not been used, their throughput is defined as Float.MAX_VALUE
   *
   * @param replicaSet replica set to be ranked
   * @return the nodes and their ranks
   */
  private List<Pair<TDataNodeLocation, Double>> rankLocations(
      TRegionReplicaSet replicaSet, LocationStatistics locationStatistics) {
    List<Pair<TDataNodeLocation, Double>> locations =
        new ArrayList<>(replicaSet.dataNodeLocations.size());
    // retrieve throughput of each node
    double totalThroughput = 0.0;
    for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
      // use Float.MAX_VALUE so that they can be added together
      Statistic statistic = locationStatistics.getStatistic(dataNodeLocation);
      double throughput;
      long lastHitTime = statistic.getLastHitTime();
      long elapsedTime = System.currentTimeMillis() - lastHitTime;
      if (lastHitTime > 0 && elapsedTime > resampleThresholdMS) {
        throughput =
            statistic.getThroughput() * Math.pow(2, elapsedTime * 1.0 / resampleThresholdMS);
      } else {
        throughput = statistic.getThroughput();
      }
      locations.add(new Pair<>(dataNodeLocation, throughput));
      totalThroughput += throughput;
    }
    if (logger.isInfoEnabled()) {
      logger.debug(
          "Location throughput: {}",
          locations.stream()
              .map(p -> new Pair<>(p.left.getDataNodeId(), p.right))
              .collect(Collectors.toList()));
      logger.debug(
          "Total throughput: {}, first rank {}",
          totalThroughput,
          locations.get(0).right / totalThroughput);
    }

    // calculate cumulative ranks
    locations.get(0).right = locations.get(0).right / totalThroughput;
    for (int i = 1; i < locations.size(); i++) {
      Pair<TDataNodeLocation, Double> location = locations.get(i);
      location.right = location.right / totalThroughput + locations.get(i - 1).right;
    }
    if (logger.isInfoEnabled()) {
      logger.debug(
          "Location ranks: {}",
          locations.stream()
              .map(p -> new Pair<>(p.left.getDataNodeId(), p.right))
              .collect(Collectors.toList()));
    }
    return locations;
  }

  private Pair<TDataNodeLocation, Double> chooseNextLocation(
      List<Pair<TDataNodeLocation, Double>> locations) {
    int chosen = 0;
    double dice = random.nextDouble();
    for (int i = 1; i < locations.size(); i++) {
      if (locations.get(i - 1).right <= dice && dice < locations.get(i).right) {
        chosen = i;
      }
    }
    if (chosen == 0 && locations.size() == 3 && logger.isDebugEnabled()) {
      logger.debug(
          "Dice {}, chosen {}, ranks {}",
          dice,
          chosen,
          locations.stream()
              .map(p -> new Pair<>(p.left.getDataNodeId(), p.right))
              .collect(Collectors.toList()));
    }
    Pair<TDataNodeLocation, Double> chosenPair = locations.remove(chosen);
    // update ranks
    double newTotalRank = 0.0;
    for (Pair<TDataNodeLocation, Double> location : locations) {
      newTotalRank += location.right;
    }
    for (Pair<TDataNodeLocation, Double> location : locations) {
      location.right = location.right / newTotalRank;
    }
    logger.debug("New ranks {}", locations);
    return chosenPair;
  }

  @Override
  public Iterator<TDataNodeLocation> iterator() {
    return orderedLocations.iterator();
  }
}
