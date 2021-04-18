package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

public class EvictionEntry {

  EvictionEntry pre;
  EvictionEntry next;
  MNode value;

  public EvictionEntry(MNode mNode) {
    value = mNode;
    mNode.setEvictionEntry(this);
  }

  EvictionEntry getPre() {
    return pre;
  }

  EvictionEntry getNext() {
    return next;
  }

  MNode getValue() {
    return value;
  }

  void setPre(EvictionEntry pre) {
    this.pre = pre;
  }

  void setNext(EvictionEntry next) {
    this.next = next;
  }

  void setValue(MNode mNode){
    value=mNode;
  }
}
