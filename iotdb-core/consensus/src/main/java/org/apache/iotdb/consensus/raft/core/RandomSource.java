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

package org.apache.iotdb.consensus.raft.core;

import java.util.Random;

/** Injected randomness so the kernel stays deterministic under test. */
public interface RandomSource {

  /** A value in {@code [0, boundExclusive)}. */
  long nextLong(long boundExclusive);

  static RandomSource seeded(long seed) {
    final Random r = new Random(seed);
    return bound -> bound <= 1 ? 0 : (r.nextLong() & Long.MAX_VALUE) % bound;
  }

  /** Always zero: election delay is always the minimum. */
  RandomSource ZERO = bound -> 0;
}
