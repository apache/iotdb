package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreedyRegionAllocatorTest {

  @Test
  public void testAllocateRegion() {
    GreedyRegionAllocator greedyRegionAllocator = new GreedyRegionAllocator();
    List<TDataNodeInfo> registeredDataNodes =
        Lists.newArrayList(
            new TDataNodeInfo(new TDataNodeLocation(1, null, null, null, null, null), 0, 0),
            new TDataNodeInfo(new TDataNodeLocation(2, null, null, null, null, null), 0, 0),
            new TDataNodeInfo(new TDataNodeLocation(3, null, null, null, null, null), 0, 0));
    List<TRegionReplicaSet> allocatedRegions = new ArrayList<>();
    List<TConsensusGroupId> tConsensusGroupIds =
        Lists.newArrayList(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0),
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 2),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 3),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 4),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 5));
    for (TConsensusGroupId tConsensusGroupId : tConsensusGroupIds) {
      TRegionReplicaSet newRegion =
          greedyRegionAllocator.allocateRegion(
              registeredDataNodes, allocatedRegions, 1, tConsensusGroupId);
      allocatedRegions.add(newRegion);
    }

    Map<TDataNodeLocation, Integer> countMap = new HashMap<>();
    for (TDataNodeInfo dataNodeInfo : registeredDataNodes) {
      countMap.put(dataNodeInfo.getLocation(), 0);
    }

    for (TRegionReplicaSet regionReplicaSet : allocatedRegions) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        countMap.computeIfPresent(dataNodeLocation, (dataNode, count) -> (count + 1));
      }
    }

    Assert.assertTrue(countMap.values().stream().mapToInt(e -> e).max().getAsInt() <= 2);
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(0).getDataNodeLocations(),
            allocatedRegions.get(1).getDataNodeLocations()));
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(2).getDataNodeLocations(),
            allocatedRegions.get(3).getDataNodeLocations()));
    Assert.assertTrue(
        Collections.disjoint(
            allocatedRegions.get(4).getDataNodeLocations(),
            allocatedRegions.get(5).getDataNodeLocations()));
  }
}
