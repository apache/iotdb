/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.consensus.index;

import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * The progress index is designed to express the progress on multiple two independent causal chains.
 * For example, all writes/updates/deletions within a single consensus group form a complete causal
 * chain. Since strict total order relations can be defined on each of these causal chains, the
 * progress index is considered to be composed of an n-tuple of total order relations
 * (S<sub>1</sub>,S<sub>2</sub>,S<sub>3</sub>,.... ,S<sub>n</sub>).
 *
 * <p>This class is designed to be immutable, meaning that instances of {@link ProgressIndex} can be
 * treated as value objects. Immutability ensures thread safety, as no external synchronization is
 * required for concurrent access to instances of this class. It also simplifies reasoning about the
 * state of the object as it cannot change once created.
 *
 * <p>However, if a {@link ProgressIndex} instance holds any mutable objects, like a {@link Map},
 * they must be deeply copied during construction or when exposed through accessors to maintain the
 * immutability contract. This prevents unintended modifications to the underlying mutable state
 * from affecting other parts of the program.
 */
public abstract class ProgressIndex implements Accountable {

  protected static final long LOCK_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReentrantReadWriteLock.class)
          + RamUsageEstimator.shallowSizeOfInstance(ReentrantReadWriteLock.ReadLock.class)
          + RamUsageEstimator.shallowSizeOfInstance(ReentrantReadWriteLock.WriteLock.class)
          + ((long) RamUsageEstimator.NUM_BYTES_OBJECT_HEADER << 1)
          + 64;

  /** Serialize this progress index to the given byte buffer. */
  public abstract void serialize(ByteBuffer byteBuffer);

  /** Serialize this progress index to the given output stream. */
  public abstract void serialize(OutputStream stream) throws IOException;

  /**
   * A.isAfter(B) is true if and only if every tuple member in A is strictly greater than the
   * corresponding tuple member in B in its corresponding total order relation。
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is strictly greater than the given consensus
   *     index
   */
  public abstract boolean isAfter(@Nonnull ProgressIndex progressIndex);

  /**
   * A.isAfter(B) is true if and only if every tuple member in A is the same as the corresponding
   * tuple member in B in its corresponding total order relation.
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is equal to the given progress index
   */
  public abstract boolean equals(ProgressIndex progressIndex);

  /**
   * A.equals(B) is true if and only if A, B are both {@link ProgressIndex} and A.equals(({@link
   * ProgressIndex})B) is true.
   *
   * @param obj the object to be compared
   * @return true if and only if this progress index is equal to the given object
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ProgressIndex)) {
      return false;
    }
    return this.equals((ProgressIndex) obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Define the isEqualOrAfter relation, A.isEqualOrAfter(B) if and only if each tuple member in A
   * is greater than or equal to B in the corresponding total order relation.
   *
   * <p>C = A.updateToMinimumEqualOrIsAfterProgressIndex(B) should satisfy:
   *
   * <p>C.isEqualOrAfter(A) is {@code true},
   *
   * <p>C.isEqualOrAfter(B) is {@code true},
   *
   * <p>there is no D such that C.isAfter(D) is {@code true} and D.isEqualOrAfter(A) is {@code true}
   * and D.isEqualOrAfter(B) is {@code true}.
   *
   * <p>The implementation of this function should be reflexive, that is
   * A.updateToMinimumIsAfterProgressIndex(B).equals(B.updateToMinimumIsAfterProgressIndex(A)) is
   * {@code true}
   *
   * <p>Note: this function will not modify the caller (this) and {@param progressIndex}.
   *
   * @param progressIndex the {@link ProgressIndex} to be compared
   * @return the minimum {@link ProgressIndex} after the given {@link ProgressIndex} and this {@link
   *     ProgressIndex}.
   */
  public abstract ProgressIndex updateToMinimumEqualOrIsAfterProgressIndex(
      ProgressIndex progressIndex);

  /**
   * @return the type of this {@link ProgressIndex}
   */
  public abstract ProgressIndexType getType();

  /**
   * Get the sum of the tuples of each total order relation of the {@link ProgressIndex}, which is
   * used for topological sorting of the {@link ProgressIndex}.
   *
   * @return The sum of the tuples corresponding to all the total order relations contained in the
   *     progress index.
   */
  public abstract TotalOrderSumTuple getTotalOrderSumTuple();

  public final int topologicalCompareTo(@Nonnull ProgressIndex progressIndex) {
    return getTotalOrderSumTuple().compareTo(progressIndex.getTotalOrderSumTuple());
  }

  /**
   * Blend two {@link ProgressIndex}es together, the result progress index should satisfy:
   *
   * <p>(result.equals(progressIndex1) || result.isAfter(progressIndex1)) is true
   *
   * <p>(result.equals(progressIndex2) || result.isAfter(progressIndex2)) is true
   *
   * <p>There is no R, such that R satisfies the above conditions and result.isAfter(R) is true
   *
   * <p>Note: this function will not modify {@param progressIndex1} and {@param progressIndex2}.
   *
   * @param progressIndex1 the first {@link ProgressIndex}. if it is {@code null}, the result {@link
   *     ProgressIndex} should be the second {@link ProgressIndex}. if it is a {@link
   *     MinimumProgressIndex}, the result {@link ProgressIndex} should be the second {@link
   *     ProgressIndex}. (if the second {@link ProgressIndex} is {@code null}, the result should be
   *     a {@link MinimumProgressIndex}). if it is a {@link HybridProgressIndex}, the result
   *     progress index should be the minimum {@link ProgressIndex} after the second {@link
   *     ProgressIndex} and the first {@link ProgressIndex}
   * @param progressIndex2 the second {@link ProgressIndex}. if it is {@code null}, the result
   *     {@link ProgressIndex} should be the first {@link ProgressIndex}. if it is a {@link
   *     MinimumProgressIndex}, the result {@link ProgressIndex} should be the first {@link
   *     ProgressIndex}. (if the first {@link ProgressIndex} is null, the result should be a minimum
   *     progress index). if it is a {@link HybridProgressIndex}, the result {@link ProgressIndex}
   *     should be the minimum {@link ProgressIndex} equal or after the first {@link ProgressIndex}
   *     and the second {@link ProgressIndex}
   * @return the minimum {@link ProgressIndex} after the first {@link ProgressIndex} and the second
   *     {@link ProgressIndex}.
   */
  protected static ProgressIndex blendProgressIndex(
      ProgressIndex progressIndex1, ProgressIndex progressIndex2) {
    if (progressIndex1 == null && progressIndex2 == null) {
      return MinimumProgressIndex.INSTANCE;
    }
    if (progressIndex1 == null || progressIndex1 instanceof MinimumProgressIndex) {
      return progressIndex2 == null ? MinimumProgressIndex.INSTANCE : progressIndex2;
    }
    if (progressIndex2 == null || progressIndex2 instanceof MinimumProgressIndex) {
      return progressIndex1; // progressIndex1 is not null
    }

    return progressIndex1 instanceof StateProgressIndex
        ? progressIndex1.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex2)
        : new HybridProgressIndex(progressIndex1)
            .updateToMinimumEqualOrIsAfterProgressIndex(progressIndex2);
  }

  /**
   * Each total ordered relation of the progress index should be a tuple. This class defines a way
   * to sum and compare the size of the tuples of each total ordered relation of progress index.
   * This method maintains the relationship of progress index in the isAfter relationship. It is
   * mainly used for topologically sorting the progress index.
   *
   * <p>Notice:TotalOrderSumTuple is an ordered tuple, the larger the subscript the higher the
   * weight of the element when comparing sizes, e.g. (1, 2) is larger than (2, 1).
   */
  protected static class TotalOrderSumTuple implements Comparable<TotalOrderSumTuple> {
    private final ImmutableList<Long> tuple;

    /**
     * ATTENTION: the order of the args is important, the larger the subscript the higher the weight
     * of the element when comparing sizes, e.g. (1, 2) is larger than (2, 1).
     *
     * @param args input args
     */
    public TotalOrderSumTuple(Long... args) {
      tuple = ImmutableList.copyOf(args);
    }

    public TotalOrderSumTuple(List<Long> list) {
      tuple = ImmutableList.copyOf(list);
    }

    @Override
    public int compareTo(TotalOrderSumTuple that) {
      if (that.tuple.size() != this.tuple.size()) {
        return this.tuple.size() - that.tuple.size();
      }
      for (int i = this.tuple.size() - 1; i >= 0; --i) {
        if (!this.tuple.get(i).equals(that.tuple.get(i))) {
          return this.tuple.get(i) < that.tuple.get(i) ? -1 : 1;
        }
      }
      return 0;
    }

    @Override
    public String toString() {
      return "TotalOrderSumTuple{" + "tuple=" + tuple + '}';
    }

    public static TotalOrderSumTuple sum(List<TotalOrderSumTuple> tupleList) {
      if (tupleList == null || tupleList.size() == 0) {
        return new TotalOrderSumTuple();
      }
      if (tupleList.size() == 1) {
        return tupleList.get(0);
      }

      List<Long> result =
          LongStream.range(0, tupleList.stream().mapToInt(t -> t.tuple.size()).max().getAsInt())
              .map(o -> 0)
              .boxed()
              .collect(Collectors.toList());
      tupleList.forEach(
          t ->
              IntStream.range(0, t.tuple.size())
                  .forEach(i -> result.set(i, result.get(i) + t.tuple.get(i))));
      return new TotalOrderSumTuple(result);
    }
  }
}
