package org.apache.iotdb.cluster.partition;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.rpc.thrift.Node;

/**
 * NodeRemovalResult stores the removed partition group and who will take over its slots.
 */
public class NodeRemovalResult {
  private PartitionGroup removedGroup;
  // if the removed group contains the local node, the local node should join a new group to
  // preserve the replication number
  private PartitionGroup newGroup;
  private Map<Node, List<Integer>> newSlotOwners;

  public PartitionGroup getRemovedGroup() {
    return removedGroup;
  }

  public void setRemovedGroup(PartitionGroup group) {
    this.removedGroup = group;
  }

  public Map<Node, List<Integer>> getNewSlotOwners() {
    return newSlotOwners;
  }

  public void setNewSlotOwners(
      Map<Node, List<Integer>> newSlotOwners) {
    this.newSlotOwners = newSlotOwners;
  }

  public PartitionGroup getNewGroup() {
    return newGroup;
  }

  public void setNewGroup(PartitionGroup newGroup) {
    this.newGroup = newGroup;
  }
}
