package org.apache.iotdb.db.metadata.metadisk.cache;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheEntry {

  protected IMNode value;

  /** whether the node in memory cache has been modified. default value is true */
  protected boolean isModified = true;

  protected AtomicInteger semaphore = new AtomicInteger(0);

  public CacheEntry(IMNode mNode) {
    value = mNode;
  }

  public IMNode getMNode() {
    return value;
  }

  public void setMNode(IMNode mNode) {
    value = mNode;
  }

  public boolean isModified() {
    return isModified;
  }

  public void setModified(boolean modified) {
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
