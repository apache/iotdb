package org.apache.iotdb.confignode.manager.allocator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Allocate Region by CopySet algorithm.
 * Reference: https://www.usenix.org/conference/atc13/technical-sessions/presentation/cidon
 */
public class CopySetRegionAllocator implements IRegionAllocator {

  private static final int maximumRandomNum = 10;

  private int maxId;
  private int weightSum;
  private Map<Integer, Integer> countMap;
  private TreeMap<Integer, TDataNodeLocation> weightMap;

  public CopySetRegionAllocator() {
    // Empty constructor
  }

  @Override
  public TRegionReplicaSet allocateRegion(List<TDataNodeLocation> onlineDataNodes, List<TRegionReplicaSet> allocatedRegions, int replicationFactor) {
    TRegionReplicaSet result = null;

    // Build weightMap for weighted random
    buildWeightMap(onlineDataNodes, allocatedRegions);

    boolean accepted = false;
    for (int retry = 0; retry < maximumRandomNum; retry++) {
      result = genWeightedRandomRegion(replicationFactor);
      if (intersectionCheck(allocatedRegions, result)) {
        accepted = true;
        break;
      }
    }

    if (!accepted) {
      result = genWeightedRandomRegion(replicationFactor);
    }

    clear();
    return result;
  }

  private void buildWeightMap(List<TDataNodeLocation> onlineDataNodes, List<TRegionReplicaSet> allocatedRegions) {
    countMap = new HashMap<>();
    weightMap = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : allocatedRegions) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        countMap.putIfAbsent(dataNodeLocation.getDataNodeId(), 0);
        countMap.put(dataNodeLocation.getDataNodeId(), countMap.get(dataNodeLocation.getDataNodeId()) + 1);
      }
    }

    maxId = 0;
    weightSum = 0;
    int maximumCount = Collections.max(countMap.values());
    for (Map.Entry<Integer, Integer> countEntry : countMap.entrySet()) {
      maxId = Math.max(maxId, countEntry.getKey());
      weightSum += maximumCount - countEntry.getValue() + 1;

      for (TDataNodeLocation dataNodeLocation : onlineDataNodes) {
        if (dataNodeLocation.getDataNodeId() == countEntry.getKey()) {
          // Entry in weightMap will be like: (prefixWeightSum, TDataNodeLocation).
          weightMap.put(weightSum, dataNodeLocation);
          break;
        }
      }
    }
  }

  private TRegionReplicaSet genWeightedRandomRegion(int replicationFactor) {
    Random random = new Random();
    Set<Integer> checkSet = new HashSet<>();
    TRegionReplicaSet randomRegion = new TRegionReplicaSet();
    for (int i = 0; i < replicationFactor; i++) {
      // Find the Entry of weightMap, that has the least key value,
      // and whose key(prefixWeightSum) greater than the random value
      Map.Entry<Integer, TDataNodeLocation> nextDataNode = weightMap.ceilingEntry(random.nextInt(weightSum));
      if (checkSet.contains(nextDataNode.getValue().getDataNodeId())) {
        i = i - 1;
        continue;
      }
      checkSet.add(nextDataNode.getValue().getDataNodeId());
      randomRegion.addToDataNodeLocations(nextDataNode.getValue());
    }
    return randomRegion;
  }

  private boolean intersectionCheck(List<TRegionReplicaSet> allocatedRegions, TRegionReplicaSet newRegion) {
    BitSet newBit = new BitSet(maxId + 1);
    for (TDataNodeLocation dataNodeLocation : newRegion.getDataNodeLocations()) {
      newBit.set(dataNodeLocation.getDataNodeId());
    }

    for (TRegionReplicaSet allocatedRegion : allocatedRegions) {
      BitSet allocatedBit = new BitSet(maxId + 1);
      for (TDataNodeLocation dataNodeLocation : allocatedRegion.getDataNodeLocations()) {
        allocatedBit.set(dataNodeLocation.getDataNodeId());
      }

      allocatedBit.and(newBit);
      if (allocatedBit.cardinality() > 1) {
        // In order to ensure the maximum scatter width and the minimum disaster rate
        return false;
      }
    }
    return true;
  }

  private void clear() {
    countMap.clear();
    countMap = null;
    weightMap.clear();
    weightMap = null;
  }
}
