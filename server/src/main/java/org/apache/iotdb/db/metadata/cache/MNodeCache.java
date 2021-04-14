package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.tsfile.common.cache.Cache;

import java.util.Collection;

public interface MNodeCache extends Cache<String, MNode> {

  int DEFAULT_MAX_CAPACITY = 10000;

  void put(String path, MNode mNode);

  boolean contains(String path);

  MNode get(String path);

  Collection<MNode> getAll();

  void remove(String path);

  int size();

  void clear();
}
