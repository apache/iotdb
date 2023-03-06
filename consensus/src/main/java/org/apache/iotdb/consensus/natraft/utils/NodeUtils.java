package org.apache.iotdb.consensus.natraft.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.consensus.common.Peer;

public class NodeUtils {

  public static List<Peer> computeAddedNodes(List<Peer> oldNodes, List<Peer> newNodes) {
    List<Peer> addedNode = new ArrayList<>();
    for (Peer newNode : newNodes) {
      if (!oldNodes.contains(newNode)) {
        addedNode.add(newNode);
      }
    }
    return addedNode;
  }

  public static Collection<Peer> unionNodes(List<Peer> currNodes, List<Peer> newNodes) {
    if (newNodes == null) {
      return currNodes;
    }
    Set<Peer> nodeUnion = new HashSet<>();
    nodeUnion.addAll(currNodes);
    nodeUnion.addAll(newNodes);
    return nodeUnion;
  }

}
