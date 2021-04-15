package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class LRUCache implements MNodeCache {

  private final int CacheCapacity;
  private final Map<String, CacheEntry> map;
  private CacheEntry first;
  private CacheEntry last;

  private final Lock lock = new ReentrantLock();

  public LRUCache(int size) {
    this.CacheCapacity = size;
    map = new HashMap<>(size);
  }

  public void put(String path, MNode mNode) {
    try {
      lock.lock();
      CacheEntry node = map.get(path);
      if (node == null) {
        if (map.size() >= CacheCapacity) {
          evictLast();
        }
        node = new CacheEntry(mNode);
      }
      node.value = mNode;
      moveToFirst(node);
      map.put(path, node);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean contains(String path) {
    try {
      lock.lock();
      return map.containsKey(path);
    } finally {
      lock.unlock();
    }
  }

  public MNode get(String path) {
    try {
      lock.lock();
      CacheEntry node = map.get(path);
      if (node == null) {
        return null;
      }
      moveToFirst(node);
      return node.value;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<MNode> getAll() {
    try {
      lock.lock();
      return map.values().stream().map(o -> o.value).collect(Collectors.toList());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(String path) {
    try {
      lock.lock();
      CacheEntry entry = map.get(path);
      if (entry != null) {
        removeEntryFromLinkedList(entry);
        map.remove(path);
        removeRecursively(entry.value);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    try {
      lock.lock();
      return map.size();
    } finally {
      lock.unlock();
    }
  }

  public void clear() {
    try {
      lock.lock();
      first = null;
      last = null;
      map.clear();
    } finally {
      lock.unlock();
    }
  }

  private void moveToFirst(CacheEntry node) {
    if (first == null || last == null) { // empty linked list
      first = last = node;
      return;
    }

    if (first == node) {
      return;
    }
    if (node.pre != null) {
      node.pre.next = node.next;
    }
    if (node.next != null) {
      node.next.pre = node.pre;
    }

    if (node == last) {
      last = last.pre;
    }

    node.next = first;
    first.pre = node;
    first = node;
    first.pre = null;
  }

  private void removeEntryFromLinkedList(CacheEntry entry) {
    if (entry != null) {
      if (entry.pre != null) {
        entry.pre.next = entry.next;
      }
      if (entry.next != null) {
        entry.next.pre = entry.pre;
      }
      if (entry == first) {
        first = entry.next;
      }
      if (entry == last) {
        last = entry.pre;
      }
    }
  }

  private void evictLast() {
    if (last != null) {
      MNode mNode = last.value;
      if (!MNode.isNull(mNode.getParent())) {
        mNode.getParent().evictChild(mNode.getName());
      }
      remove(last.getKey());
    }
  }

  private void removeRecursively(MNode mNode) {
    remove(mNode.getFullPath());
    for (MNode child : mNode.getChildren().values()) {
      removeRecursively(child);
    }
  }

  private static class CacheEntry {
    CacheEntry pre;
    CacheEntry next;
    MNode value;

    CacheEntry(MNode mNode) {
      value = mNode;
    }

    String getKey() {
      return value.getFullPath();
    }
  }
}
