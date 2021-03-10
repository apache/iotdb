package org.apache.iotdb.db.metadata.mnodecache;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.tsfile.common.cache.Cache;

public interface MNodeCache extends Cache<PartialPath, MNode> {

  void putMNode(MNode mNode);

  void removeMNode(PartialPath path);
}
