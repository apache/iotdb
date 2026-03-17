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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Strata Estimator for estimating the number of differences between two sets before sizing the main
 * IBF. Uses a stack of small IBFs at different strata levels, where each element is assigned to a
 * stratum based on the number of trailing zeros in its key. Communication cost: ~50 KB.
 *
 * <p>Reference: Eppstein et al., "What's the Difference? Efficient Set Reconciliation without Prior
 * Context"
 */
public class StrataEstimator {

  private static final int STRATA_COUNT = 32;
  private static final int CELLS_PER_STRATUM = 80;
  private static final int HASH_COUNT = 3;

  private final InvertibleBloomFilter[] strata;

  public StrataEstimator() {
    strata = new InvertibleBloomFilter[STRATA_COUNT];
    for (int i = 0; i < STRATA_COUNT; i++) {
      strata[i] = new InvertibleBloomFilter(CELLS_PER_STRATUM, HASH_COUNT);
    }
  }

  private StrataEstimator(InvertibleBloomFilter[] strata) {
    this.strata = strata;
  }

  /** Insert a (key, valueHash) pair into the appropriate stratum. */
  public void insert(long key, long valueHash) {
    int level = Long.numberOfTrailingZeros(key);
    if (level >= STRATA_COUNT) {
      level = STRATA_COUNT - 1;
    }
    strata[level].insert(key, valueHash);
  }

  /**
   * Estimate the number of differences between this estimator and another.
   *
   * @param other the other side's strata estimator
   * @return estimated diff count
   */
  public long estimateDifference(StrataEstimator other) {
    long estimate = 0;

    for (int i = STRATA_COUNT - 1; i >= 0; i--) {
      InvertibleBloomFilter diff = this.strata[i].subtract(other.strata[i]);
      IBFDecodeResult result = diff.decode();
      if (!result.isSuccess()) {
        // If decode fails at this level, scale up the partial count
        return (estimate + result.getPartialCount()) * (1L << (i + 1));
      }
      estimate += result.getPartialCount();
    }

    return estimate;
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeInt(STRATA_COUNT);
    for (InvertibleBloomFilter ibf : strata) {
      ibf.serialize(out);
    }
  }

  public static StrataEstimator deserialize(ByteBuffer buffer) {
    int count = buffer.getInt();
    InvertibleBloomFilter[] strata = new InvertibleBloomFilter[count];
    for (int i = 0; i < count; i++) {
      strata[i] = InvertibleBloomFilter.deserialize(buffer);
    }
    return new StrataEstimator(strata);
  }

  /** Approximate serialized size: 4 + STRATA_COUNT * (8 + CELLS_PER_STRATUM * 20) ≈ 50 KB. */
  public int serializedSize() {
    int size = 4;
    for (InvertibleBloomFilter ibf : strata) {
      size += ibf.serializedSize();
    }
    return size;
  }
}
