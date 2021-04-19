package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;

public interface CacheStrategy {

  int getSize();

  void applyChange(MNode mNode);

  void setModified(MNode mNode, boolean modified);

  void remove(MNode mNode);

  Collection<MNode> evict();
}
