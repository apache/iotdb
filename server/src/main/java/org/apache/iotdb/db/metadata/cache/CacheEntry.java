package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

public class CacheEntry {

  CacheEntry pre;
  CacheEntry next;
  MNode value;

  /** whether the node in memory cache has been modified. default value is true */
  boolean isModified=true;

  CacheEntry(MNode mNode) {
    value = mNode;
    mNode.setEvictionEntry(this);
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

  void setValue(MNode mNode){
    value=mNode;
  }

  boolean isModified() {
    return isModified;
  }

  void setModified(boolean modified) {
    isModified = modified;
  }
}
