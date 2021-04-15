package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;

public interface EvictionStrategy {

  void applyChange(MNode mNode);

  int remove(MNode mNode);

  void replace(MNode oldMNode, MNode newMNode);

  Collection<MNode> evict();
}
