package org.apache.iotdb.db.query.udf.datastructure;

import org.apache.iotdb.db.mpp.transformation.datastructure.NewCache;

import java.util.Arrays;

public class NewLRUCache extends NewCache {

  private final int[] inMemory;
  private final int[] outMemory;

  public NewLRUCache(int capacity) {
    super(capacity);
    inMemory = new int[capacity << 4];
    outMemory = new int[capacity << 4];
    Arrays.fill(inMemory, Integer.MIN_VALUE);
    Arrays.fill(outMemory, Integer.MIN_VALUE);
  }

  public int get(int targetIndex) {
    access(targetIndex);
    return inMemory[targetIndex];
  }

  public void set(int targetIndex, int value) {
    access(targetIndex);
    inMemory[targetIndex] = value;
  }

  private void access(int targetIndex) {
    if (!containsKey(targetIndex)) {
      if (cacheCapacity <= size()) {
        int lastIndex = getLast();
        outMemory[lastIndex] = inMemory[lastIndex];
        inMemory[lastIndex] = Integer.MIN_VALUE;
      }
      inMemory[targetIndex] = outMemory[targetIndex];
      outMemory[targetIndex] = Integer.MIN_VALUE;
    }
    put(targetIndex, targetIndex);
  }
}
