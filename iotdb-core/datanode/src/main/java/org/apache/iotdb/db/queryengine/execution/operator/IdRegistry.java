package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.IntArrayFIFOQueue;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.function.IntFunction;

public class IdRegistry<T> {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(IdRegistry.class);

  private final HashMap<Integer, T> objects = new HashMap<>();
  private final IntFIFOQueue emptySlots = new IntFIFOQueue();

  /**
   * Provides a new ID referencing the provided object.
   *
   * @return ID referencing the provided object
   */
  public T allocateId(IntFunction<T> factory) {
    T result;
    if (emptySlots.size() != 0) {
      int id = emptySlots.dequeueInt();
      result = factory.apply(id);
      objects.put(id, result);
    } else {
      result = factory.apply(objects.size());
      objects.put(objects.size(), result);
    }
    return result;
  }

  public void deallocate(int id) {
    objects.remove(id);
    emptySlots.enqueue(id);
  }

  public T get(int id) {
    return objects.get(id);
  }

  /** Does not include the sizes of the referenced objects themselves. */
  public long sizeOf() {
    return INSTANCE_SIZE + RamUsageEstimator.sizeOfMap(objects) + emptySlots.sizeOf();
  }

  private static class IntFIFOQueue extends IntArrayFIFOQueue {
    private static final long INSTANCE_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(IntFIFOQueue.class);

    public long sizeOf() {
      return INSTANCE_SIZE + RamUsageEstimator.sizeOf(array);
    }
  }
}
