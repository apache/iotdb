package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache implements MNodeCache {

  private final LinkedHashMap<String, MNode> map;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  public LRUCache() {
    this(MNodeCache.DEFAULT_MAX_CAPACITY);
  }

  public LRUCache(int maxCapacity) {
    map =
        new LinkedHashMap<String, MNode>(maxCapacity, DEFAULT_LOAD_FACTOR, true) {
          protected boolean removeEldestEntry(Map.Entry<String, MNode> eldest) {
            return maxCapacity < map.size();
          }
        };
  }

  @Override
  public void put(String path,MNode mNode) {
    try {
      writeLock.lock();
      map.put(path.toString(), mNode);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean contains(String path) {
    try {
      readLock.lock();
      return map.containsKey(path.toString());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public MNode get(String path) {
    try {
      readLock.lock();
      return map.get(path.toString());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Collection<MNode> getAll() {
    try {
      readLock.lock();
      return map.values();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void remove(String path) {
    try {
      writeLock.lock();
      map.remove(path.toString());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void clear() {
    try {
      writeLock.lock();
      map.clear();
    } finally {
      writeLock.unlock();
    }
  }
}
