package org.apache.iotdb.confignode.manager.load.balancer.router;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;

/**
 * The IRouter is a functional interface, which means a new functional class who implements the
 * IRouter must be created for generating the latest real-time routing policy.
 */
public interface IRouter {

  /**
   * Generate an optimal real-time read/write requests routing policy.
   *
   * @param replicaSets All RegionReplicasEts currently owned by the cluster
   * @return Map<TConsensusGroupId, TRegionReplicaSet>, The routing policy of read/write requests
   *     for each Region is based on the order in the TRegionReplicaSet. The replica with higher
   *     sorting result have higher priority.
   */
  Map<TConsensusGroupId, TRegionReplicaSet> genRealTimeRoutingPolicy(
      List<TRegionReplicaSet> replicaSets);
}
