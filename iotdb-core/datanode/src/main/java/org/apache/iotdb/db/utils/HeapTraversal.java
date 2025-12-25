package org.apache.iotdb.db.utils;

import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class HeapTraversal {
  public enum Child {
    LEFT,
    RIGHT
  }

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HeapTraversal.class);
  private static final long TOP_BIT_MASK = 1L << (Long.SIZE - 1);

  private long shifted;
  private int treeDepthToNode;

  public void resetWithPathTo(long targetNodeIndex) {
    checkArgument(targetNodeIndex >= 1, "Target node index must be greater than or equal to one");
    int leadingZeros = Long.numberOfLeadingZeros(targetNodeIndex);
    // Shift off the leading zeros PLUS the most significant one bit (which is not needed for this
    // calculation)
    shifted = targetNodeIndex << (leadingZeros + 1);
    treeDepthToNode = Long.SIZE - (leadingZeros + 1);
  }

  public boolean isTarget() {
    return treeDepthToNode == 0;
  }

  public Child nextChild() {
    checkState(!isTarget(), "Already at target");
    Child childToFollow = (shifted & TOP_BIT_MASK) == 0 ? Child.LEFT : Child.RIGHT;
    shifted <<= 1;
    treeDepthToNode--;
    return childToFollow;
  }

  public long sizeOf() {
    return INSTANCE_SIZE;
  }
}
