package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
      moveToFirst(mNode);
    } finally {
      lock.unlock();
    }
  }

  private void moveToFirst(MNode mNode){
    if (MNode.isNull(mNode)) {
      return;
    }
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
  }

  @Override
  public int remove(MNode mNode) {
    if (MNode.isNull(mNode)) {
      return 0;
    }
    try {
      lock.lock();
      return removeRecursively(mNode);
    }finally {
      lock.unlock();
    }
  }

  @Override
  public void replace(MNode oldMNode, MNode newMNode) {
    EvictionEntry entry=oldMNode.getEvictionEntry();
    newMNode.setEvictionEntry(entry);
    entry.setValue(newMNode);
    applyChange(newMNode);
  }

  private void removeOne(MNode mNode){
    if (MNode.isNull(mNode)) {
      return;
    }
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
  }

  private int removeRecursively(MNode mNode){
    removeOne(mNode);
    int num=1;
    for (MNode child : mNode.getChildren().values()) {
      num+=removeRecursively(child);
    }
    return num;
  }

  @Override
  public Collection<MNode> evict() {
    try {
      lock.lock();
      List<MNode> evictedMNode=new LinkedList<>();
      if(first==null){
        return evictedMNode;
      }
      MNode mNode=first.getValue();
      if(mNode.getParent()!=null){
        mNode.getParent().evictChild(mNode.getName());
      }
      removeRecursively(mNode);
      flatten(mNode,evictedMNode);
      return evictedMNode;
    }finally {
      lock.unlock();
    }
  }

  private void flatten(MNode mNode, List<MNode> list){
    list.add(mNode);
    for(MNode child:mNode.getChildren().values()){
      flatten(child,list);
    }
  }

}
