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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array;

import org.apache.iotdb.calc.execution.operator.source.relational.Percentile;

import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;

public final class PercentileBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(PercentileBigArray.class);
  private final ObjectBigArray<Percentile> array;

  public PercentileBigArray() {
    array = new ObjectBigArray<>();
  }

  /**
   * Unlike fixed-size sketches (e.g. TDigest), each {@link Percentile} stores all raw values and
   * grows unboundedly as values are appended through {@link #get(long)}. Caching the retained size
   * and only refreshing it on {@code set} would therefore drift far below the real footprint, so we
   * sum the live estimated size of every Percentile on demand to keep memory accounting accurate.
   */
  public long sizeOf() {
    long[] sizeOfPercentile = {0};
    array.forEach(
        item -> {
          if (item != null) {
            sizeOfPercentile[0] += item.getEstimatedSize();
          }
        });
    return INSTANCE_SIZE + shallowSizeOf(array) + sizeOfPercentile[0];
  }

  public Percentile get(long index) {
    Percentile percentile = array.get(index);
    if (percentile == null) {
      percentile = new Percentile();
      array.set(index, percentile);
    }
    return percentile;
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void reset() {
    array.forEach(
        item -> {
          if (item != null) {
            item.clear();
          }
        });
  }
}
