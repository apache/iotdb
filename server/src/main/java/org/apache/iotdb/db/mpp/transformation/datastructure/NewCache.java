package org.apache.iotdb.db.mpp.transformation.datastructure;

import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class NewCache extends LinkedHashMap<Integer, Integer> {

  protected final int cacheCapacity;

  protected NewCache (int cacheCapacity) {
    super(cacheCapacity, 0.75F, true);
    this.cacheCapacity = cacheCapacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<Integer, Integer> eldest) {
    return size() > cacheCapacity;
  }

  // get the eldest key
  public int getLast() {
    return this.entrySet().iterator().next().getKey();
  }
}
