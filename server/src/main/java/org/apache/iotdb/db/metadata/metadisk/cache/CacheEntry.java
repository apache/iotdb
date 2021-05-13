package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheEntry {

  CacheEntry pre = null;
  CacheEntry next = null;
  MNode value;

  /** whether the node in memory cache has been modified. default value is true */
  boolean isModified = true;

  AtomicInteger semaphore = new AtomicInteger(0);

  CacheEntry(MNode mNode) {
    value = mNode;
    mNode.setCacheEntry(this);
  }

  CacheEntry getPre() {
    return pre;
  }

  CacheEntry getNext() {
    return next;
  }

  MNode getValue() {
    return value;
  }

  void setPre(CacheEntry pre) {
    this.pre = pre;
  }

  void setNext(CacheEntry next) {
    this.next = next;
  }

  void setValue(MNode mNode) {
    value = mNode;
  }

  boolean isModified() {
    return isModified;
  }

  void setModified(boolean modified) {
    isModified = modified;
  }

  public boolean isLocked() {
    return semaphore.get() > 0;
  }

  public void increaseLock() {
    semaphore.getAndIncrement();
  }

  public void decreaseLock() {
    if (semaphore.get() > 0) {
      semaphore.getAndDecrement();
    }
  }
}
