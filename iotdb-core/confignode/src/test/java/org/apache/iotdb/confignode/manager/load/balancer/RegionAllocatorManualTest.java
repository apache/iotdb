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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyCopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.RandomLeaderBalancer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class RegionAllocatorManualTest {

  private Logger LOGGER = LoggerFactory.getLogger(RegionAllocatorManualTest.class);

  private static final int TEST_LOOP = 10;
  private static final int MIN_DATA_NODE_NUM = 3;
  private static final int MAX_DATA_NODE_NUM = 100;
  private static final int MIN_DATA_REGION_PER_DATA_NODE = 6;
  private static final int MAX_DATA_REGION_PER_DATA_NODE = 6;
  private static final int DATA_REPLICATION_FACTOR = 3;
  private static final double EXAM_LOOP = 100000;
  private static final String DATABASE = "root.db";

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

  // private static final IRegionGroupAllocator ALLOCATOR = new GreedyRegionGroupAllocator();
  private static final ILeaderBalancer BALANCER = new RandomLeaderBalancer();

  public static class DataEntry {
    public final Integer countRange;
    public final Integer minScatterWidth;
    public final List<Double> disabledPercent;
    public final Integer leaderRange;

    private DataEntry(
        int countRange, int minScatterWidth, List<Double> disabledPercent, int leaderRange) {
      this.countRange = countRange;
      this.minScatterWidth = minScatterWidth;
      this.disabledPercent = disabledPercent;
      this.leaderRange = leaderRange;
    }
  }

  @Test
  public void loopTest() throws IOException {
    List<DataEntry> testResult = new ArrayList<>();
    for (int dataNodeNum = MIN_DATA_NODE_NUM; dataNodeNum <= MAX_DATA_NODE_NUM; dataNodeNum++) {
      for (int dataRegionPerDataNode = MIN_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode <= MAX_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode++) {
        ConfigNodeDescriptor.getInstance()
            .getConf()
            .setDataRegionPerDataNode(dataRegionPerDataNode);
        ConfigNodeDescriptor.getInstance()
            .getConf()
            .setDefaultDataRegionGroupNumPerDatabase(
                dataRegionPerDataNode * dataNodeNum * DATA_REPLICATION_FACTOR);
        testResult.add(test(dataNodeNum, dataRegionPerDataNode));
      }
      LOGGER.info("{}, finish", dataNodeNum);
    }

    final String path = "/Users/yongzaodan/Desktop/simulation/";
    String allocatorPath = "GCR";
    //     FileWriter countW = new FileWriter(path + "gcr-simulate/r=2/" + allocatorPath + "-w");
    //     FileWriter scatterW = new FileWriter(path + "gcr-simulate/r=2/" + allocatorPath + "-s");
    //     FileWriter percentW = new FileWriter(path + "percent/" + allocatorPath);
    FileWriter leaderW =
        new FileWriter(path + "cdf-simulate/r=3/" + BALANCER.getClass().getSimpleName());

    for (DataEntry entry : testResult) {
      //            countW.write(entry.countRange.toString());
      //            countW.write("\n");
      //            countW.flush();
      //      scatterW.write(entry.minScatterWidth.toString());
      //      scatterW.write("\n");
      //      scatterW.flush();
      //            for (Double percent : entry.disabledPercent) {
      //              percentW.write(percent.toString());
      //              percentW.write("\n");
      //              percentW.flush();
      //            }
      leaderW.write(entry.leaderRange.toString());
      leaderW.write("\n");
      leaderW.flush();
    }

    //        countW.close();
    //    scatterW.close();
    //        percentW.close();
    leaderW.close();
  }

  public DataEntry test(int TEST_DATA_NODE_NUM, int DATA_REGION_PER_DATA_NODE) {
    // Construct TEST_DATA_NODE_NUM DataNodes
    Random random = new Random();
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }

    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;
    List<Integer> regionCountList = new ArrayList<>();
    List<Integer> scatterWidthList = new ArrayList<>();
    List<Integer> leaderCountList = new ArrayList<>();
    int[] hitList = new int[20];
    Arrays.fill(hitList, 0);
    for (int loop = 1; loop <= TEST_LOOP; loop++) {
      IRegionGroupAllocator ALLOCATOR = new GreedyCopySetRegionGroupAllocator();
      /* Allocate RegionGroup */
      List<TRegionReplicaSet> allocateResult = new ArrayList<>();
      for (int index = 0; index < dataRegionGroupNum; index++) {
        allocateResult.add(
            ALLOCATOR.generateOptimalRegionReplicasDistribution(
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                allocateResult,
                allocateResult,
                DATA_REPLICATION_FACTOR,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, index)));
      }

      Set<List<Integer>> schemaHashSet = new HashSet<>();
      for (TRegionReplicaSet result : allocateResult) {
        schemaHashSet.add(
            result.getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .sorted()
                .collect(Collectors.toList()));
      }
      //      for (int M = 1; M <= 0.1 * TEST_DATA_NODE_NUM; M++) {
      //        for (int exam = 0; exam < EXAM_LOOP; exam++) {
      //          Set<Integer> examSet = new TreeSet<>();
      //          while (examSet.size() < DATA_REPLICATION_FACTOR) {
      //            int id = random.nextInt(TEST_DATA_NODE_NUM) + 1;
      //            while (examSet.contains(id)) {
      //              id = random.nextInt(TEST_DATA_NODE_NUM) + 1;
      //            }
      //            examSet.add(id);
      //          }
      //          hitList[M] +=
      // schemaHashSet.contains(examSet.stream().sorted().collect(Collectors.toList())) ? 1 : 0;
      //        }
      //      }

      /* Count Region in each DataNode */
      // Map<DataNodeId, RegionGroup Count>
      Map<Integer, Integer> regionCounter = new TreeMap<>();
      allocateResult.forEach(
          regionReplicaSet ->
              regionReplicaSet
                  .getDataNodeLocations()
                  .forEach(
                      dataNodeLocation ->
                          regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));

      /* Calculate scatter width for each DataNode */
      // Map<DataNodeId, ScatterWidth>
      // where a true in the bitset denotes the corresponding DataNode can help the DataNode in
      // Map-Key to share the RegionGroup-leader and restore data when restarting.
      // The more true in the bitset, the more safety the cluster DataNode in Map-Key is.
      Map<Integer, BitSet> scatterWidthMap = new TreeMap<>();
      for (TRegionReplicaSet replicaSet : allocateResult) {
        for (int i = 0; i < DATA_REPLICATION_FACTOR; i++) {
          for (int j = i + 1; j < DATA_REPLICATION_FACTOR; j++) {
            int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
            int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
            scatterWidthMap.computeIfAbsent(dataNodeId1, empty -> new BitSet()).set(dataNodeId2);
            scatterWidthMap.computeIfAbsent(dataNodeId2, empty -> new BitSet()).set(dataNodeId1);
          }
        }
      }
      int scatterWidthSum = 0;
      int minScatterWidth = Integer.MAX_VALUE;
      int maxScatterWidth = Integer.MIN_VALUE;
      for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
        int scatterWidth =
            scatterWidthMap.containsKey(i) ? scatterWidthMap.get(i).cardinality() : 0;
        scatterWidthSum += scatterWidth;
        minScatterWidth = Math.min(minScatterWidth, scatterWidth);
        maxScatterWidth = Math.max(maxScatterWidth, scatterWidth);
        regionCountList.add(regionCounter.getOrDefault(i, 0));
        scatterWidthList.add(scatterWidth);
      }

      /* Balance Leader */
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap =
          Collections.singletonMap(
              DATABASE,
              allocateResult.stream()
                  .map(TRegionReplicaSet::getRegionId)
                  .collect(Collectors.toList()));
      Map<TConsensusGroupId, TRegionReplicaSet> regionReplicaSetMap =
          allocateResult.stream().collect(Collectors.toMap(TRegionReplicaSet::getRegionId, r -> r));
      Map<TConsensusGroupId, Integer> optimalLeaderDistribution =
          BALANCER.generateOptimalLeaderDistribution(
              databaseRegionGroupMap, regionReplicaSetMap, new TreeMap<>(), new TreeSet<>());
      // Map<DataNodeId, Leader Count>
      Map<Integer, Integer> leaderCounter = new TreeMap<>();
      optimalLeaderDistribution.forEach(
          (regionId, leaderId) -> leaderCounter.merge(leaderId, 1, Integer::sum));
      int minLeaderCount = leaderCounter.values().stream().min(Integer::compareTo).orElse(0);
      int maxLeaderCount = leaderCounter.values().stream().max(Integer::compareTo).orElse(0);
      leaderCounter.forEach((dataNodeId, leaderCount) -> leaderCountList.add(leaderCount));
    }

    List<Double> percentList = new ArrayList<>();
    for (int M = 1; M <= 10; M++) {
      percentList.add(hitList[M] / EXAM_LOOP / (double) TEST_LOOP);
    }
    return new DataEntry(
        regionCountList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - regionCountList.stream().mapToInt(Integer::intValue).min().orElse(0),
        scatterWidthList.stream().mapToInt(Integer::intValue).min().orElse(0),
        percentList,
        leaderCountList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - leaderCountList.stream().mapToInt(Integer::intValue).min().orElse(0));
  }
}
