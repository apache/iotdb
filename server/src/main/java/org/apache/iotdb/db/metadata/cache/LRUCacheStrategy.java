package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MNodeImpl;

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
    if (MNodeImpl.isNull(mNode)) {
      return;
    }
    try {
      lock.lock();
      if (MNodeImpl.isNull(mNode)) {
        return;
      }
      if (!MNodeImpl.isNull(mNode.getParent()) && !mNode.getParent().isCached()) {
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
    if (MNodeImpl.isNull(mNode) || mNode.getCacheEntry() == null) {
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
    if (MNodeImpl.isNull(mNode)) {
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
    if (MNodeImpl.isNull(mNode)) {
      return;
    }
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
    if (MNodeImpl.isNull(mNode)) {
      return;
    }
    CacheEntry entry = mNode.getCacheEntry();
    if (entry == null) {
      return;
    }
    removeOne(entry);
    mNode.setCacheEntry(null);
    if (removedMNodes != null && entry.isModified) {
      removedMNodes.add(mNode);
    }
    for (MNode child : mNode.getChildren().values()) {
      evictRecursivelyAndCollectModified(child, removedMNodes);
    }
  }

  @Override
  public Collection<MNode> evict() {
    try {
      lock.lock();
      List<MNode> evictedMNode = new LinkedList<>();
      if (last == null) {
        return evictedMNode;
      }
      MNode mNode = last.value;
      if (mNode.getParent() != null) {
        mNode.getParent().evictChild(mNode.getName());
      }
      MNode parent = mNode.getParent();
      while (parent != null && parent.getCacheEntry().isModified) {
        evictedMNode.add(parent);
        parent = parent.getParent();
      }
      evictRecursivelyAndCollectModified(mNode, evictedMNode);
      return evictedMNode;
    } finally {
      lock.unlock();
    }
  }
}
