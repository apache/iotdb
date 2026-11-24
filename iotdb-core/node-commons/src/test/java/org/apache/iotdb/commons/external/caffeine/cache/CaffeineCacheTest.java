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

package org.apache.iotdb.commons.external.caffeine.cache;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CaffeineCacheTest {

  @Test
  public void testAllStrongCombinations() {
    int combinations = 0;
    for (int evict = 0; evict < 3; evict++) {
      for (boolean expAccess : new boolean[] {false, true}) {
        for (boolean expWrite : new boolean[] {false, true}) {
          for (boolean stats : new boolean[] {false, true}) {
            for (int listener = 0; listener < 3; listener++) {
              Caffeine<Object, Object> builder = Caffeine.newBuilder().executor(Runnable::run);
              if (evict == 1) {
                builder.maximumSize(8);
              } else if (evict == 2) {
                builder.maximumWeight(64).weigher((String k, String v) -> k.length() + v.length());
              }
              if (expAccess) {
                builder.expireAfterAccess(10, TimeUnit.SECONDS);
              }
              if (expWrite) {
                builder.expireAfterWrite(10, TimeUnit.SECONDS);
              }
              if (stats) {
                builder.recordStats();
              }

              Caffeine<String, String> typedBuilder;
              if (listener == 1) {
                typedBuilder = builder.removalListener((String k, String v, RemovalCause c) -> {});
              } else if (listener == 2) {
                typedBuilder = builder.evictionListener((String k, String v, RemovalCause c) -> {});
              } else {
                @SuppressWarnings({"unchecked", "rawtypes"})
                Caffeine<String, String> cast = (Caffeine) builder;
                typedBuilder = cast;
              }

              Cache<String, String> cache = typedBuilder.build();
              cache.put("k1", "v1");
              Assert.assertEquals("v1", cache.getIfPresent("k1"));
              Assert.assertEquals("v2", cache.get("k2", k -> "v2"));
              cache.policy().eviction().ifPresent(e -> e.setMaximum(4));
              cache.invalidate("k1");
              Assert.assertNull(cache.getIfPresent("k1"));
              cache.invalidateAll();
              cache.cleanUp();

              LoadingCache<String, String> loadingCache = typedBuilder.build(k -> "val_" + k);
              Assert.assertEquals("val_a", loadingCache.get("a"));
              if (stats) {
                Assert.assertTrue(loadingCache.stats().requestCount() >= 1);
              }
              loadingCache.cleanUp();
              combinations++;
            }
          }
        }
      }
    }
    Assert.assertEquals(72, combinations);
  }

  @Test
  public void testExpirationEvictionAndDynamicResize() {
    AtomicLong nanos = new AtomicLong(0L);
    List<RemovalCause> causes = new ArrayList<>();

    Cache<String, String> cache =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .ticker(nanos::get)
            .maximumWeight(10)
            .weigher((String k, String v) -> 5)
            .expireAfterWrite(10, TimeUnit.SECONDS)
            .recordStats()
            .removalListener((String k, String v, RemovalCause cause) -> causes.add(cause))
            .build();

    cache.put("a", "1");
    cache.put("b", "2");
    Assert.assertEquals("1", cache.getIfPresent("a"));
    Assert.assertEquals(1.0, cache.stats().hitRate(), 1e-6);

    // Resize down to 5 -> should evict 1 entry due to SIZE
    cache.policy().eviction().ifPresent(e -> e.setMaximum(5));
    Assert.assertEquals(5L, cache.policy().eviction().get().getMaximum());
    Assert.assertTrue(causes.contains(RemovalCause.SIZE));

    // Advance clock past 10s -> remaining entry expires
    nanos.addAndGet(TimeUnit.SECONDS.toNanos(11));
    Assert.assertNull(cache.getIfPresent("a"));
    Assert.assertNull(cache.getIfPresent("b"));
    cache.cleanUp();
    Assert.assertTrue(causes.contains(RemovalCause.EXPIRED));
  }
}
