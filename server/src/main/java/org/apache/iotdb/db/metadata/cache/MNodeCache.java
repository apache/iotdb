package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.tsfile.common.cache.Cache;

import java.util.Collection;

public interface MNodeCache extends Cache<PartialPath, MNode> {

  void put(MNode mNode);

  MNode get(PartialPath path);

  Collection<MNode> getAll();

  void remove(PartialPath path);

  void clear();
}
