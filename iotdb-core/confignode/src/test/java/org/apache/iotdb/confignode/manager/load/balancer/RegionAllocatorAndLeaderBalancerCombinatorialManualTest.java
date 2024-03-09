package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyCopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.MinCostFlowLeaderBalancer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class RegionAllocatorAndLeaderBalancerCombinatorialManualTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RegionAllocatorAndLeaderBalancerCombinatorialManualTest.class);

  private static final int TEST_LOOP = 1;
  private static final int TEST_DATA_NODE_NUM = 100;
  private static final int DATA_REGION_PER_DATA_NODE = 4;
  private static final int DATA_REPLICATION_FACTOR = 3;
  private static final String DATABASE = "root.db";

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

  private static final IRegionGroupAllocator ALLOCATOR = new GreedyCopySetRegionGroupAllocator();
  //      new TieredReplicationAllocator(
  //          TEST_DATA_NODE_NUM, DATA_REPLICATION_FACTOR, DATA_REGION_PER_DATA_NODE);
  private static final ILeaderBalancer BALANCER = new MinCostFlowLeaderBalancer();

  @BeforeClass
  public static void setUp() {
    // Construct TEST_DATA_NODE_NUM DataNodes
    Random random = new Random();
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }
  }

  @Test
  public void manualTest() {
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;
    List<Integer> regionCountList = new ArrayList<>();
    List<Integer> scatterWidthList = new ArrayList<>();
    List<Integer> leaderCountList = new ArrayList<>();
    for (int loop = 1; loop <= TEST_LOOP; loop++) {
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
      //      LOGGER.info(
      //          "Loop: {}, Test :{}, {}",
      //          loop,
      //          ALLOCATOR.getClass().getSimpleName(),
      //          BALANCER.getClass().getSimpleName());
      //      LOGGER.info(
      //          "Allocate {} DataRegionGroups for {} DataNodes", dataRegionGroupNum,
      // TEST_DATA_NODE_NUM);
      //      LOGGER.info(
      //          "Scatter width avg: {}, min: {}, max: {}",
      //          (double) scatterWidthSum / TEST_DATA_NODE_NUM,
      //          minScatterWidth,
      //          maxScatterWidth);

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
      // LOGGER.info("Leader count min: {}, max: {}", minLeaderCount, maxLeaderCount);
    }

    LOGGER.info("All tests done.");
    double regionCountAvg =
        regionCountList.stream().mapToInt(Integer::intValue).average().orElse(0);
    double regionCountVariance =
        regionCountList.stream()
                .mapToInt(Integer::intValue)
                .mapToDouble(i -> Math.pow(i - regionCountAvg, 2))
                .sum()
            / regionCountList.size();
    int regionCountRange =
        regionCountList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - regionCountList.stream().mapToInt(Integer::intValue).min().orElse(0);
    LOGGER.info(
        "Region count avg: {}, var: {}, range: {}",
        regionCountAvg,
        regionCountVariance,
        regionCountRange);
    double scatterWidthAvg =
        scatterWidthList.stream().mapToInt(Integer::intValue).average().orElse(0);
    double scatterWidthVariance =
        scatterWidthList.stream()
                .mapToInt(Integer::intValue)
                .mapToDouble(i -> Math.pow(i - scatterWidthAvg, 2))
                .sum()
            / scatterWidthList.size();
    int scatterWidthRange =
        scatterWidthList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - scatterWidthList.stream().mapToInt(Integer::intValue).min().orElse(0);
    LOGGER.info(
        "Scatter width avg: {}, var: {}, range: {}",
        scatterWidthAvg,
        scatterWidthVariance,
        scatterWidthRange);
    double leaderCountAvg =
        leaderCountList.stream().mapToInt(Integer::intValue).average().orElse(0);
    double leaderCountVariance =
        leaderCountList.stream()
                .mapToInt(Integer::intValue)
                .mapToDouble(i -> Math.pow(i - leaderCountAvg, 2))
                .sum()
            / leaderCountList.size();
    int leaderCountRange =
        leaderCountList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - leaderCountList.stream().mapToInt(Integer::intValue).min().orElse(0);
    LOGGER.info(
        "Leader count avg: {}, var: {}, range: {}",
        leaderCountAvg,
        leaderCountVariance,
        leaderCountRange);
  }
}
