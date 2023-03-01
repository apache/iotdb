package org.apache.iotdb.consensus.natraft.utils;

import java.util.ArrayList;
import java.util.List;
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
}
