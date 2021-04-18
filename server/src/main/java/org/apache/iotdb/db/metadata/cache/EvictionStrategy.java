package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;

public interface EvictionStrategy {

  int getSize();

  void applyChange(MNode mNode);

  void remove(MNode mNode);

  Collection<MNode> evict();
}
