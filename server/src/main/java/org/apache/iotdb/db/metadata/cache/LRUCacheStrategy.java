package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCacheStrategy implements CacheStrategy {

  private int size = 0;

  private CacheEntry first;
  private CacheEntry last;

  private final Lock lock = new ReentrantLock();

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void applyChange(MNode mNode) {
    if (mNode==null) {
      return;
    }
    try {
      lock.lock();
      if (mNode.getParent()!=null && !mNode.getParent().isCached()) {
        return;
      }
      CacheEntry entry = mNode.getCacheEntry();
      if (entry == null) {
        size++;
        entry = new CacheEntry(mNode);
      }
      moveToFirst(entry);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void setModified(MNode mNode, boolean modified) {
    if (mNode==null || mNode.getCacheEntry() == null) {
      return;
    }
    mNode.getCacheEntry().setModified(modified);
  }

  private void moveToFirst(CacheEntry entry) {

    if (first == null || last == null) { // empty linked list
      first = last = entry;
      return;
    }

    if (first == entry) {
      return;
    }
    if (entry.pre != null) {
      entry.pre.next = entry.next;
    }
    if (entry.next != null) {
      entry.next.pre = entry.pre;
    }

    if (entry == last) {
      last = last.pre;
    }

    entry.next = first;
    first.pre = entry;
    first = entry;
    first.pre = null;
  }

  @Override
  public void remove(MNode mNode) {
    if (mNode==null||!mNode.isCached()) {
      return;
    }
    try {
      lock.lock();
      removeRecursively(mNode);
    } finally {
      lock.unlock();
    }
  }

  private void removeOne(CacheEntry entry) {
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
    size--;
  }

  private void removeRecursively(MNode mNode) {
    CacheEntry entry = mNode.getCacheEntry();
    if (entry == null) {
      return;
    }
    removeOne(entry);
    mNode.setCacheEntry(null);
    for (MNode child : mNode.getChildren().values()) {
      removeRecursively(child);
    }
  }

  private void evictRecursivelyAndCollectModified(MNode mNode, Collection<MNode> removedMNodes) {
    for (MNode child : mNode.getChildren().values()) {
      CacheEntry entry = child.getCacheEntry();
      if (entry == null) {
        continue;
      }
      removeOne(entry);
      child.setCacheEntry(null);
      evictRecursivelyAndCollectModified(child, removedMNodes);
      if (entry.isModified) {
        removedMNodes.add(child);
      }
    }
  }

  @Override
  public List<MNode> evict() {
    try {
      lock.lock();
      List<MNode> evictedMNode = new LinkedList<>();
      if (last == null) {
        return evictedMNode;
      }

      MNode mNode = last.value;

      MNode parent = mNode.getParent();
      while (parent != null && parent.isCached()&& parent.getCacheEntry().isModified) {
//        evictedMNode.add(parent);
        mNode=parent;
        parent = parent.getParent();
      }

      removeOne(mNode.getCacheEntry());
      evictRecursivelyAndCollectModified(mNode, evictedMNode);
      evictedMNode.add(mNode);
      return evictedMNode;
    } finally {
      lock.unlock();
    }
  }
}
