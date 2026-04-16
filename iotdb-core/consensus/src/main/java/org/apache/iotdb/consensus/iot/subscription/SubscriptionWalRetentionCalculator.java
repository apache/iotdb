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

package org.apache.iotdb.consensus.iot.subscription;

import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;

import java.util.Collection;

public class SubscriptionWalRetentionCalculator {

  public static final class SubscriptionRetentionBound {

    private static final long RETAIN_ALL_SEARCH_INDEX = Long.MIN_VALUE + 1;
    private static final long RETAIN_ALL_VERSION_ID = 0L;

    private final long safelyDeletedSearchIndex;
    private final long retainedMinVersionId;

    private SubscriptionRetentionBound(
        final long safelyDeletedSearchIndex, final long retainedMinVersionId) {
      this.safelyDeletedSearchIndex = safelyDeletedSearchIndex;
      this.retainedMinVersionId = retainedMinVersionId;
    }

    private static SubscriptionRetentionBound noConstraint() {
      return new SubscriptionRetentionBound(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    private static SubscriptionRetentionBound retainAll() {
      return new SubscriptionRetentionBound(RETAIN_ALL_SEARCH_INDEX, RETAIN_ALL_VERSION_ID);
    }

    private static SubscriptionRetentionBound of(
        final long safelyDeletedSearchIndex, final long retainedMinVersionId) {
      return new SubscriptionRetentionBound(safelyDeletedSearchIndex, retainedMinVersionId);
    }

    private SubscriptionRetentionBound mergeDeleteEither(final SubscriptionRetentionBound other) {
      return new SubscriptionRetentionBound(
          Math.max(safelyDeletedSearchIndex, other.safelyDeletedSearchIndex),
          Math.max(retainedMinVersionId, other.retainedMinVersionId));
    }

    private SubscriptionRetentionBound mergeDeleteOnlyIfBoth(
        final SubscriptionRetentionBound other) {
      return new SubscriptionRetentionBound(
          Math.min(safelyDeletedSearchIndex, other.safelyDeletedSearchIndex),
          Math.min(retainedMinVersionId, other.retainedMinVersionId));
    }

    public long getSafelyDeletedSearchIndex() {
      return safelyDeletedSearchIndex;
    }

    public long getRetainedMinVersionId() {
      return retainedMinVersionId;
    }
  }

  private final ConsensusReqReader consensusReqReader;

  public SubscriptionWalRetentionCalculator(final ConsensusReqReader consensusReqReader) {
    this.consensusReqReader = consensusReqReader;
  }

  public SubscriptionRetentionBound calculate(
      final Collection<SubscriptionWalRetentionPolicy> retentionPolicies) {
    SubscriptionRetentionBound mergedBound = SubscriptionRetentionBound.noConstraint();
    for (final SubscriptionWalRetentionPolicy policy : retentionPolicies) {
      // For each topic, data can be deleted once either its size retention or its time retention
      // allows reclamation. Across topics sharing the same region WAL, we can only delete data
      // that every active topic already allows us to reclaim.
      final SubscriptionRetentionBound perQueueBound =
          buildSizeRetentionBound(policy.getRetentionBytes())
              .mergeDeleteEither(buildTimeRetentionBound(policy.getRetentionMs()));
      mergedBound = mergedBound.mergeDeleteOnlyIfBoth(perQueueBound);
    }
    return mergedBound;
  }

  private SubscriptionRetentionBound buildSizeRetentionBound(final long retentionSizeLimit) {
    if (retentionSizeLimit == SubscriptionWalRetentionPolicy.UNBOUNDED || retentionSizeLimit <= 0) {
      return SubscriptionRetentionBound.retainAll();
    }

    final long regionWalSize = consensusReqReader.getRegionDiskUsage();
    if (regionWalSize <= retentionSizeLimit) {
      return SubscriptionRetentionBound.retainAll();
    }

    final long excess = regionWalSize - retentionSizeLimit;
    return SubscriptionRetentionBound.of(
        consensusReqReader.getSearchIndexToFreeAtLeast(excess),
        consensusReqReader.getVersionIdToFreeAtLeast(excess));
  }

  private SubscriptionRetentionBound buildTimeRetentionBound(final long retentionMs) {
    if (retentionMs == SubscriptionWalRetentionPolicy.UNBOUNDED || retentionMs <= 0) {
      return SubscriptionRetentionBound.retainAll();
    }

    final long cutoffTimeMs = System.currentTimeMillis() - retentionMs;
    return SubscriptionRetentionBound.of(
        consensusReqReader.getSearchIndexToFreeBeforeTimestamp(cutoffTimeMs),
        consensusReqReader.getVersionIdToFreeBeforeTimestamp(cutoffTimeMs));
  }
}
