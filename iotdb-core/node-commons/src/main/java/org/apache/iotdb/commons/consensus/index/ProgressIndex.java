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

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * The progress index is designed to express the progress on multiple two independent causal chains.
 * For example, all writes/updates/deletions within a single consensus group form a complete causal
 * chain. Since strict total order relations can be defined on each of these causal chains, the
 * progress index is considered to be composed of an n-tuple of total order relations
 * (S<sub>1</sub>,S<sub>2</sub>,S<sub>3</sub>,.... ,S<sub>n</sub>).
 */
public abstract class ProgressIndex {

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

  /**
   * Define the isEqualOrAfter relation, A.isEqualOrAfter(B) if and only if each tuple member in A
   * is greater than or equal to B in the corresponding total order relation.
   *
   * <p>C = A.updateToMinimumIsAfterProgressIndex(B) should be satisfied:
   *
   * <p>C.isEqualOrAfter(A) is true,
   *
   * <p>C.isEqualOrAfter(B) is true
   *
   * <p>there is no D such that C.isEqualOrAfter(D) is true.
   *
   * <p>The implementation of this function should be reflexive, that is
   * A.updateToMinimumIsAfterProgressIndex(B).equals(B.updateToMinimumIsAfterProgressIndex(A)) is
   * true
   *
   * <p>Note: this function may modify the caller.
   *
   * @param progressIndex the progress index to be compared
   * @return the minimum progress index after the given progress index and this progress index
   */
  public abstract ProgressIndex updateToMinimumIsAfterProgressIndex(ProgressIndex progressIndex);

  /** @return the type of this progress index */
  public abstract ProgressIndexType getType();

  /**
   * Get the sum of the tuples of each total order relation of the progress index, which is used for
   * topological sorting of the progress index.
   *
   * @return The sum of the tuples corresponding to all the total order relations contained in the
   *     progress index.
   */
  public abstract TotalOrderSumTuple getTotalOrderSumTuple();

  public final int topologicalCompareTo(ProgressIndex progressIndex) {
    return progressIndex == null
        ? 1
        : getTotalOrderSumTuple().compareTo(progressIndex.getTotalOrderSumTuple());
  }

  /**
   * Blend two progress index together, the result progress index should satisfy:
   *
   * <p>(result.equals(progressIndex1) || result.isAfter(progressIndex1)) is true
   *
   * <p>(result.equals(progressIndex2) || result.isAfter(progressIndex2)) is true
   *
   * <p>There is no R, such that R satisfies the above conditions and result.isAfter(R) is true
   *
   * @param progressIndex1 the first progress index. if it is null, the result progress index should
   *     be the second progress index. if it is a minimum progress index, the result progress index
   *     should be the second progress index. (if the second progress index is null, the result
   *     should be a minimum progress index). if it is a hybrid progress index, the result progress
   *     index should be the minimum progress index after the second progress index and the first
   *     progress index
   * @param progressIndex2 the second progress index. if it is null, the result progress index
   *     should be the first progress index. if it is a minimum progress index, the result progress
   *     index should be the first progress index. (if the first progress index is null, the result
   *     should be a minimum progress index). if it is a hybrid progress index, the result progress
   *     index should be the minimum progress index after the first progress index and the second
   *     progress index
   * @return the minimum progress index after the first progress index and the second progress index
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

    return new HybridProgressIndex(progressIndex1)
        .updateToMinimumIsAfterProgressIndex(progressIndex2);
  }

  /**
   * Each total ordered relation of the progress index should be a tuple. This class defines a way
   * to sum and compare the size of the tuples of each total ordered relation of progress index.
   * This method maintains the relationship of progress index in the isAfter relationship. It is
   * mainly used for topologically sorting the progress index.
   *
   * <p>Notice:
   */
  protected static class TotalOrderSumTuple implements Comparable<TotalOrderSumTuple> {
    private final ImmutableList<Long> tuple;

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
