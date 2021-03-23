package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache implements MNodeCache {

  private final LinkedHashMap<String, MNode> map;
  private final Lock lock = new ReentrantLock();
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  public LRUCache() {
    this(MNodeCache.DEFAULT_MAX_CAPACITY);
  }

  public LRUCache(int maxCapacity) {
    map =
        new LinkedHashMap<String, MNode>(maxCapacity, DEFAULT_LOAD_FACTOR, true) {
          protected boolean removeEldestEntry(Map.Entry<String, MNode> eldest) {
            return maxCapacity + 1 == map.size();
          }
        };
  }

  @Override
  public void put(MNode mNode) {
    try {
      lock.lock();
      map.put(mNode.getFullPath(), mNode);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public MNode get(PartialPath path) {
    try {
      lock.lock();
      return map.get(path.toString());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<MNode> getAll() {
    try {
      lock.lock();
      return map.values();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(PartialPath path) {
    try {
      lock.lock();
      map.remove(path.toString());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    try {
      lock.lock();
      map.clear();
    } finally {
      lock.unlock();
    }
  }
}
