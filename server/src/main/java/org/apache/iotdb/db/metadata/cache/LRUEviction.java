package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUEviction implements EvictionStrategy {

  private EvictionEntry first;
  private EvictionEntry last;

  private final Lock lock = new ReentrantLock();

  @Override
  public void applyChange(MNode mNode) {
    if (MNode.isNull(mNode)) {
      return;
    }
    try {
      lock.lock();
      EvictionEntry entry = mNode.getEvictionEntry();
      if (entry == null) {
        entry = new EvictionEntry(mNode);
      }
      if (first == null || last == null) { // empty linked list
        first = last = entry;
        return;
      }

      if (first == entry) {
        return;
      }
      if (entry.getPre() != null) {
        entry.getPre().setNext(entry.getNext());
      }
      if (entry.getNext() != null) {
        entry.getNext().setPre(entry.getPre());
      }

      if (entry == last) {
        last = last.getPre();
      }

      entry.setNext(first);
      first.setPre(entry);
      first = entry;
      first.setPre(null);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(MNode mNode) {
    if (MNode.isNull(mNode)) {
      return;
    }
    try {
      lock.lock();
      EvictionEntry entry = mNode.getEvictionEntry();
      if (entry == null) {
        return;
      }
      if (entry.getPre() != null) {
        entry.getPre().setNext(entry.getNext());
      }
      if (entry.getNext() != null) {
        entry.getNext().setPre(entry.getPre());
      }
      if (entry == first) {
        first = entry.getNext();
      }
      if (entry == last) {
        last = entry.getPre();
      }
    } finally {
      lock.unlock();
    }
    for (MNode child : mNode.getChildren().values()) {
      remove(child);
    }
  }

  @Override
  public Collection<MNode> evict() {
    return null;
  }
}
