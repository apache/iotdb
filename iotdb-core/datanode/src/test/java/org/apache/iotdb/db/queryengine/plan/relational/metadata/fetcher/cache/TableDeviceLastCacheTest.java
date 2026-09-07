/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

public class TableDeviceLastCacheTest {

  @Test
  public void testLazyUpdateOnlyComposesCachedMeasurements() {
    final TableDeviceLastCache cache = new TableDeviceLastCache(false);
    cache.initOrInvalidate(null, null, new String[] {"s1"}, false);
    final AtomicInteger composedValueCount = new AtomicInteger();

    cache.tryUpdate(
        new String[] {"s1", "s2"},
        null,
        new LastCacheUpdateSource() {
          @Override
          public long getLastCacheTimestamp() {
            return 1L;
          }

          @Override
          public boolean hasLastCacheValue(final int index) {
            return true;
          }

          @Override
          public TimeValuePair getLastCacheValue(final int index) {
            composedValueCount.incrementAndGet();
            return new TimeValuePair(1L, new TsPrimitiveType.TsInt(index + 1));
          }
        });

    Assert.assertEquals(1, composedValueCount.get());
    Assert.assertEquals(
        new TimeValuePair(1L, new TsPrimitiveType.TsInt(1)), cache.getTimeValuePair("s1"));
    Assert.assertNull(cache.getTimeValuePair("s2"));
  }

  @Test
  public void testKnownNullTimePreservesHistoricalValueAndClearsOnNewerValue() {
    final TableDeviceLastCache cache = new TableDeviceLastCache(false);

    cache.initOrInvalidate(null, null, new String[] {"", "s1"}, false);

    final TimeValuePair historicalValue = new TimeValuePair(1L, new TsPrimitiveType.TsInt(1));
    cache.tryUpdate(
        new String[] {"", "s1"},
        new TimeValuePair[] {
          new TimeValuePair(1L, TableDeviceLastCache.PLACEHOLDER_NO_VALUE), historicalValue
        });
    cache.tryUpdate(
        new String[] {"s1"},
        new TimeValuePair[] {new TimeValuePair(2L, TableDeviceLastCache.PLACEHOLDER_NO_VALUE)});

    Assert.assertEquals(historicalValue, cache.getTimeValuePair("s1"));
    Optional<Pair<OptionalLong, TsPrimitiveType[]>> result =
        cache.getLastRow("", Collections.singletonList("s1"));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(OptionalLong.of(2L), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {TableDeviceLastCache.PLACEHOLDER_NO_VALUE}, result.get().getRight());

    final TimeValuePair newerValue = new TimeValuePair(3L, new TsPrimitiveType.TsInt(3));
    cache.tryUpdate(new String[] {"s1"}, new TimeValuePair[] {newerValue});

    Assert.assertEquals(newerValue, cache.getTimeValuePair("s1"));
    result = cache.getLastRow("", Collections.singletonList("s1"));
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(OptionalLong.of(3L), result.get().getLeft());
    Assert.assertArrayEquals(
        new TsPrimitiveType[] {new TsPrimitiveType.TsInt(3)}, result.get().getRight());
  }

  @Test
  public void testInvalidateMeasurementClearsKnownNullTimeAndAlignedTime() {
    final TableDeviceLastCache cache = new TableDeviceLastCache(false);

    cache.initOrInvalidate(null, null, new String[] {"", "s1"}, false);
    cache.tryUpdate(
        new String[] {"", "s1"},
        new TimeValuePair[] {
          new TimeValuePair(2L, TableDeviceLastCache.PLACEHOLDER_NO_VALUE),
          new TimeValuePair(2L, TableDeviceLastCache.PLACEHOLDER_NO_VALUE)
        });

    Assert.assertTrue(cache.getLastRow("", Collections.singletonList("s1")).isPresent());
    Assert.assertNotNull(cache.getTimeValuePair(""));

    cache.invalidate("s1");

    Assert.assertFalse(cache.getLastRow("", Collections.singletonList("s1")).isPresent());
    Assert.assertNull(cache.getTimeValuePair(""));
    Assert.assertNull(cache.getTimeValuePair("s1"));
  }
}
