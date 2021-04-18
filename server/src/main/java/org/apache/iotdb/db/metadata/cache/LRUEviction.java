package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUEviction implements EvictionStrategy {

  private int size=0;

  private EvictionEntry first;
  private EvictionEntry last;

  private final Lock lock = new ReentrantLock();

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void applyChange(MNode mNode) {
    if (MNode.isNull(mNode)) {
      return;
    }
    try {
      lock.lock();
      if (MNode.isNull(mNode)) {
        return;
      }
      if(!MNode.isNull(mNode.getParent())&&!mNode.getParent().isCached()){
        return;
      }
      EvictionEntry entry = mNode.getEvictionEntry();
      if (entry == null) {
        size++;
        entry = new EvictionEntry(mNode);
      }
      moveToFirst(entry);
    } finally {
      lock.unlock();
    }
  }

  private void moveToFirst(EvictionEntry entry){

    if (first == null || last == null) { // empty linked list
      first = last = entry;
      return;
    }

    if (first == entry) {
      return;
    }
    if (entry.pre != null) {
      entry.pre.next=entry.next;
    }
    if (entry.next != null) {
      entry.next.pre=entry.pre;
    }

    if (entry == last) {
      last = last.pre;
    }

    entry.next=first;
    first.pre=entry;
    first = entry;
    first.pre=null;
  }

  @Override
  public void remove(MNode mNode) {
    if (MNode.isNull(mNode)) {
      return;
    }
    try {
      lock.lock();
      size-=removeRecursively(mNode,null);
    }finally {
      lock.unlock();
    }
  }

  private void removeOne(EvictionEntry entry){
    if (entry.pre != null) {
      entry.pre.next=entry.next;
    }
    if (entry.next != null) {
      entry.next.pre=entry.pre;
    }
    if (entry == first) {
      first = entry.next;
    }
    if (entry == last) {
      last = entry.pre;
    }
  }

  private int removeRecursively(MNode mNode, Collection<MNode> removedMNodes){
    if (MNode.isNull(mNode)) {
      return 0;
    }
    EvictionEntry entry = mNode.getEvictionEntry();
    if (entry == null) {
      return 0;
    }
    removeOne(entry);
    mNode.setEvictionEntry(null);
    if(removedMNodes!=null){
      removedMNodes.add(mNode);
    }
    int num=1;
    for (MNode child : mNode.getChildren().values()) {
      num+=removeRecursively(child,removedMNodes);
    }
    return num;
  }

  @Override
  public Collection<MNode> evict() {
    try {
      lock.lock();
      List<MNode> evictedMNode=new LinkedList<>();
      if(last==null){
        return evictedMNode;
      }
      MNode mNode=last.value;
      if(mNode.getParent()!=null){
        mNode.getParent().evictChild(mNode.getName());
      }
      size-=removeRecursively(mNode,evictedMNode);
      return evictedMNode;
    }finally {
      lock.unlock();
    }
  }

}
