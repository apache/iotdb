package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.List;

/** this interface provides operations on cache */
public interface CacheStrategy {

  /** get the size of the current cache */
  int getSize();

  /** change the mnode's position in cache */
  void applyChange(MNode mNode);

  /**
   * change the mnode's status in cache if a mnode in cache is modified, it will be collected when
   * eviction is triggered and need to be persisted
   */
  void setModified(MNode mNode, boolean modified);

  /** remove a mnode from cache, so as its subtree */
  void remove(MNode mNode);

  /**
   * evict a mnode and remove its subtree from the cache and collect the modified mnodes in cache
   * the evicted one will be the first one of the returned collection and the rest of the returned
   * collection need to be persisted
   */
  List<MNode> evict();

  List<MNode> collectModified(MNode mNode);

  void clear();
}
