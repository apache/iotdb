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
import org.apache.iotdb.consensus.iot.subscription.SubscriptionWalRetentionCalculator.SubscriptionRetentionBound;

import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SubscriptionWalRetentionCalculatorTest {

  @Test
  public void testCommittedProgressKeepsMoreWalThanTopicRetention() {
    final SubscriptionWalRetentionCalculator calculator =
        new SubscriptionWalRetentionCalculator(new RetentionTestConsensusReqReader());
    final SubscriptionRetentionBound bound =
        calculator.calculate(
            Collections.singletonList(
                new SubscriptionWalRetentionPolicy(
                    "topic", 100L, SubscriptionWalRetentionPolicy.UNBOUNDED)),
            Arrays.asList(8L, 4L));

    assertEquals(100L, bound.getSafelyDeletedSearchIndex());
    assertEquals(4L, bound.getRetainedMinVersionId());
  }

  @Test
  public void testTopicRetentionKeepsMoreWalThanCommittedProgress() {
    final SubscriptionWalRetentionCalculator calculator =
        new SubscriptionWalRetentionCalculator(new RetentionTestConsensusReqReader());
    final SubscriptionRetentionBound bound =
        calculator.calculate(
            Collections.singletonList(
                new SubscriptionWalRetentionPolicy(
                    "topic", 100L, SubscriptionWalRetentionPolicy.UNBOUNDED)),
            Collections.singletonList(20L));

    assertEquals(100L, bound.getSafelyDeletedSearchIndex());
    assertEquals(10L, bound.getRetainedMinVersionId());
  }

  private static final class RetentionTestConsensusReqReader implements ConsensusReqReader {

    @Override
    public void setSafelyDeletedSearchIndex(final long safelyDeletedSearchIndex) {}

    @Override
    public ReqIterator getReqIterator(final long startIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCurrentSearchIndex() {
      return 0L;
    }

    @Override
    public long getCurrentWALFileVersion() {
      return 0L;
    }

    @Override
    public long getTotalSize() {
      return 200L;
    }

    @Override
    public long getRegionDiskUsage() {
      return 200L;
    }

    @Override
    public Pair<Long, Long> getDeletionBoundToFreeAtLeast(final long bytesToFree) {
      return new Pair<>(100L, 10L);
    }
  }
}
