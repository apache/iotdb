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

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface ProgressIndex {

  /** serialize this progress index to the given byte buffer */
  void serialize(ByteBuffer byteBuffer);

  /** serialize this progress index to the given output stream */
  void serialize(OutputStream stream) throws IOException;

  /**
   * A.isAfter(B) is true if and only if A is strictly greater than B.
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is strictly greater than the given consensus
   *     index
   */
  boolean isAfter(ProgressIndex progressIndex);

  /**
   * A.equals(B) is true if and only if A is equal to B
   *
   * @param progressIndex the progress index to be compared
   * @return true if and only if this progress index is equal to the given progress index
   */
  boolean equals(ProgressIndex progressIndex);

  /**
   * A.equals(B) is true if and only if A is equal to B
   *
   * @param obj the object to be compared
   * @return true if and only if this progress index is equal to the given object
   */
  @Override
  boolean equals(Object obj);

  /**
   * C = A.updateToMinimumIsAfterProgressIndex(B) where C should satisfy:
   *
   * <p>(C.equals(A) || C.isAfter(A)) is true
   *
   * <p>(C.equals(B) || C.isAfter(B)) is true
   *
   * <p>There is no D, such that D satisfies the above conditions and C.isAfter(D) is true
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
  ProgressIndex updateToMinimumIsAfterProgressIndex(@Nonnull ProgressIndex progressIndex);

  static ProgressIndex blendProgressIndex(
      ProgressIndex progressIndex, ProgressIndex anotherProgressIndex) {
    if (progressIndex instanceof MinimumProgressIndex) {
      return anotherProgressIndex;
    } else if (anotherProgressIndex instanceof MinimumProgressIndex) {
      return progressIndex;
    }

    final HybridProgressIndex hybridProgressIndex =
        new HybridProgressIndex(progressIndex.getClass().getSimpleName(), progressIndex);
    return hybridProgressIndex.updateToMinimumIsAfterProgressIndex(anotherProgressIndex);
  }
}
