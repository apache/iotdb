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
package org.apache.iotdb.confignode.manager.load.balancer.router.leader;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderBalancerComparisonTest {

  // Set this field to true, and you can see the readable test results in command line
  private static final boolean isCommandLineMode = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderBalancerComparisonTest.class);
  private static FileWriter WRITER;

  private static final GreedyLeaderBalancer GREEDY_LEADER_BALANCER = new GreedyLeaderBalancer();
  private static final MinCostFlowLeaderBalancer MIN_COST_FLOW_LEADER_BALANCER =
      new MinCostFlowLeaderBalancer();

  private static final Random RANDOM = new Random();
  private static final int TEST_MAX_DATA_NODE_NUM = 100;
  private static final int TEST_CPU_CORE_NUM = 16;
  private static final int TEST_REPLICA_NUM = 3;
  private static final double GREEDY_INIT_RATE = 0.9;
  private static final double DISABLE_DATA_NODE_RATE = 0.05;

  // Invoke this interface if you want to record the test result
  public static void prepareWriter() throws IOException {
    if (isCommandLineMode) {
      WRITER = null;
    } else {
      WRITER = new FileWriter("./leaderBalancerTest.txt");
    }
  }

  // Add @Test here to enable this test
  public void leaderBalancerComparisonTest() throws IOException {
    for (int dataNodeNum = 3; dataNodeNum <= TEST_MAX_DATA_NODE_NUM; dataNodeNum++) {
      // Simulate each DataNode has 16 CPU cores
      // and each RegionGroup has 3 replicas
      int regionGroupNum = TEST_CPU_CORE_NUM * dataNodeNum / TEST_REPLICA_NUM;
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap = new HashMap<>();
      Map<TConsensusGroupId, Integer> regionLeaderMap = new HashMap<>();
      generateTestData(dataNodeNum, regionGroupNum, regionReplicaSetMap, regionLeaderMap);

      if (isCommandLineMode) {
        LOGGER.info("============================");
        LOGGER.info("DataNodeNum: {}, RegionGroupNum: {}", dataNodeNum, regionGroupNum);
      }

      // Basic test
      Map<TConsensusGroupId, Integer> greedyLeaderDistribution = new ConcurrentHashMap<>();
      Statistics greedyStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              GREEDY_LEADER_BALANCER,
              regionReplicaSetMap,
              regionLeaderMap,
              new HashSet<>(),
              greedyLeaderDistribution);
      Map<TConsensusGroupId, Integer> mcfLeaderDistribution = new ConcurrentHashMap<>();
      Statistics mcfStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              MIN_COST_FLOW_LEADER_BALANCER,
              regionReplicaSetMap,
              regionLeaderMap,
              new HashSet<>(),
              mcfLeaderDistribution);
      if (isCommandLineMode) {
        LOGGER.info("[Basic test]");
        LOGGER.info("Greedy balancer: {}", greedyStatistics);
        LOGGER.info("MinCostFlow balancer: {}", mcfStatistics);
      } else {
        greedyStatistics.toFile();
        mcfStatistics.toFile();
      }

      // Disaster test
      int disabledDataNodeNum = (int) Math.ceil(dataNodeNum * DISABLE_DATA_NODE_RATE);
      HashSet<Integer> disabledDataNodeSet = new HashSet<>();
      while (disabledDataNodeSet.size() < disabledDataNodeNum) {
        int dataNodeId = RANDOM.nextInt(dataNodeNum);
        if (disabledDataNodeSet.contains(dataNodeId)) {
          continue;
        }
        disabledDataNodeSet.add(dataNodeId);
      }
      greedyStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              GREEDY_LEADER_BALANCER,
              regionReplicaSetMap,
              greedyLeaderDistribution,
              disabledDataNodeSet,
              greedyLeaderDistribution);
      mcfStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              MIN_COST_FLOW_LEADER_BALANCER,
              regionReplicaSetMap,
              mcfLeaderDistribution,
              disabledDataNodeSet,
              mcfLeaderDistribution);
      if (isCommandLineMode) {
        LOGGER.info("[Disaster test]");
        LOGGER.info("Greedy balancer: {}", greedyStatistics);
        LOGGER.info("MinCostFlow balancer: {}", mcfStatistics);
      } else {
        greedyStatistics.toFile();
        mcfStatistics.toFile();
      }

      // Recovery test
      greedyStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              GREEDY_LEADER_BALANCER,
              regionReplicaSetMap,
              greedyLeaderDistribution,
              new HashSet<>(),
              greedyLeaderDistribution);
      mcfStatistics =
          doBalancing(
              dataNodeNum,
              regionGroupNum,
              MIN_COST_FLOW_LEADER_BALANCER,
              regionReplicaSetMap,
              mcfLeaderDistribution,
              new HashSet<>(),
              mcfLeaderDistribution);
      if (isCommandLineMode) {
        LOGGER.info("[Recovery test]");
        LOGGER.info("Greedy balancer: {}", greedyStatistics);
        LOGGER.info("MinCostFlow balancer: {}", mcfStatistics);
      } else {
        greedyStatistics.toFile();
        mcfStatistics.toFile();
      }
    }
  }

  private void generateTestData(
      int dataNodeNum,
      int regionGroupNum,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap) {

    Map<Integer, AtomicInteger> regionCounter = new ConcurrentHashMap<>();
    Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
    for (int i = 0; i < dataNodeNum; i++) {
      regionCounter.put(i, new AtomicInteger(0));
      leaderCounter.put(i, new AtomicInteger(0));
    }

    int greedyNum = (int) (GREEDY_INIT_RATE * regionGroupNum);
    int randomNum = regionGroupNum - greedyNum;
    for (int index = 0; index < regionGroupNum; index++) {
      int leaderId = -1;
      TConsensusGroupId regionGroupId =
          new TConsensusGroupId(TConsensusGroupType.DataRegion, index);
      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet().setRegionId(regionGroupId);

      int seed = RANDOM.nextInt(greedyNum + randomNum);
      if (seed < greedyNum) {
        // Greedy pick RegionReplicas and leader
        int leaderWeight = Integer.MAX_VALUE;
        PriorityQueue<Pair<Integer, Integer>> dataNodePriorityQueue =
            new PriorityQueue<>(Comparator.comparingInt(Pair::getRight));
        regionCounter.forEach(
            (dataNodeId, regionGroupCount) ->
                dataNodePriorityQueue.offer(new Pair<>(dataNodeId, regionGroupCount.get())));
        for (int i = 0; i < TEST_REPLICA_NUM; i++) {
          int dataNodeId = Objects.requireNonNull(dataNodePriorityQueue.poll()).getLeft();
          regionReplicaSet.addToDataNodeLocations(
              new TDataNodeLocation().setDataNodeId(dataNodeId));
          if (leaderCounter.get(dataNodeId).get() < leaderWeight) {
            leaderWeight = leaderCounter.get(dataNodeId).get();
            leaderId = dataNodeId;
          }
        }
        greedyNum -= 1;
      } else {
        // Random pick RegionReplicas and leader
        Set<Integer> randomSet = new HashSet<>();
        while (randomSet.size() < TEST_REPLICA_NUM) {
          int dataNodeId = RANDOM.nextInt(dataNodeNum);
          if (randomSet.contains(dataNodeId)) {
            continue;
          }

          randomSet.add(dataNodeId);
          regionReplicaSet.addToDataNodeLocations(
              new TDataNodeLocation().setDataNodeId(dataNodeId));
        }
        leaderId = new ArrayList<>(randomSet).get(RANDOM.nextInt(TEST_REPLICA_NUM));
        randomNum -= 1;
      }

      regionReplicaSetMap.put(regionGroupId, regionReplicaSet);
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(
              dataNodeLocation ->
                  regionCounter.get(dataNodeLocation.getDataNodeId()).getAndIncrement());
      regionLeaderMap.put(regionGroupId, leaderId);
      leaderCounter.get(leaderId).getAndIncrement();
    }
  }

  private Statistics doBalancing(
      int dataNodeNum,
      int regionGroupNum,
      ILeaderBalancer leaderBalancer,
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Set<Integer> disabledDataNodeSet,
      Map<TConsensusGroupId, Integer> stableLeaderDistribution) {

    Statistics result = new Statistics();
    result.rounds = -1;
    Map<TConsensusGroupId, Integer> lastDistribution = new ConcurrentHashMap<>(regionLeaderMap);
    for (int rounds = 0; rounds < 1000; rounds++) {
      Map<TConsensusGroupId, Integer> currentDistribution =
          leaderBalancer.generateOptimalLeaderDistribution(
              regionReplicaSetMap, lastDistribution, disabledDataNodeSet);
      if (currentDistribution.equals(lastDistribution)) {
        // The leader distribution is stable
        result.rounds = rounds;
        break;
      }

      AtomicInteger switchTimes = new AtomicInteger();
      lastDistribution
          .keySet()
          .forEach(
              regionGroupId -> {
                if (!Objects.equals(
                    lastDistribution.get(regionGroupId), currentDistribution.get(regionGroupId))) {
                  switchTimes.getAndIncrement();
                }
              });

      result.switchTimes += switchTimes.get();
      lastDistribution.clear();
      lastDistribution.putAll(currentDistribution);
    }

    stableLeaderDistribution.clear();
    stableLeaderDistribution.putAll(lastDistribution);

    double sum = 0;
    double avg = (double) (regionGroupNum) / (double) (dataNodeNum);
    int minLeaderCount = Integer.MAX_VALUE;
    int maxLeaderCount = Integer.MIN_VALUE;
    Map<Integer, AtomicInteger> leaderCounter = new ConcurrentHashMap<>();
    lastDistribution.forEach(
        (regionGroupId, leaderId) ->
            leaderCounter
                .computeIfAbsent(leaderId, empty -> new AtomicInteger(0))
                .getAndIncrement());
    for (Map.Entry<Integer, AtomicInteger> entry : leaderCounter.entrySet()) {
      int leaderCount = entry.getValue().get();
      sum += Math.pow((double) leaderCount - avg, 2);
      minLeaderCount = Math.min(minLeaderCount, leaderCount);
      maxLeaderCount = Math.max(maxLeaderCount, leaderCount);
    }
    result.range = maxLeaderCount - minLeaderCount;
    result.variance = sum / (double) (dataNodeNum);

    return result;
  }

  private static class Statistics {

    // The number of execution rounds that the output of balance algorithm is stable
    private int rounds;
    // The number of change leader until the output of balance algorithm is stable
    private int switchTimes;
    // The range of the number of cluster leaders
    private int range;
    // The variance of the number of cluster leaders
    private double variance;

    private Statistics() {
      this.rounds = 0;
      this.switchTimes = 0;
      this.range = 0;
      this.variance = 0;
    }

    private void toFile() throws IOException {
      WRITER.write(
          rounds + "," + switchTimes + "," + range + "," + String.format("%.6f", variance) + "\n");
      WRITER.flush();
    }

    @Override
    public String toString() {
      return "Statistics{"
          + "rounds="
          + rounds
          + ", switchTimes="
          + switchTimes
          + ", range="
          + range
          + ", variance="
          + variance
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Statistics that = (Statistics) o;
      return rounds == that.rounds
          && switchTimes == that.switchTimes
          && range == that.range
          && Math.abs(variance - that.variance) <= 0.1;
    }

    @Override
    public int hashCode() {
      return Objects.hash(rounds, switchTimes, range, variance);
    }
  }
}
