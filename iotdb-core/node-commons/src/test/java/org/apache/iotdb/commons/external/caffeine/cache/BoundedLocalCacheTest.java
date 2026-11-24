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

import org.apache.iotdb.commons.external.caffeine.cache.BoundedLocalCache.PerformCleanupTask;
import org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness;
import org.apache.iotdb.commons.external.caffeine.cache.testing.FakeTicker;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.external.caffeine.cache.BLCHeader.DrainStatusRef.IDLE;
import static org.apache.iotdb.commons.external.caffeine.cache.BLCHeader.DrainStatusRef.PROCESSING_TO_IDLE;
import static org.apache.iotdb.commons.external.caffeine.cache.BLCHeader.DrainStatusRef.PROCESSING_TO_REQUIRED;
import static org.apache.iotdb.commons.external.caffeine.cache.BLCHeader.DrainStatusRef.REQUIRED;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for {@link BoundedLocalCache}, policies, views, spliterators, and
 * eviction.
 */
public final class BoundedLocalCacheTest {

  @Test
  public void manualAndLoadingCacheOperations() {
    AtomicInteger loads = new AtomicInteger();
    LoadingCache<Integer, String> cache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .executor(Runnable::run)
            .recordStats()
            .build(
                key -> {
                  loads.incrementAndGet();
                  if (key == -1) {
                    return null;
                  }
                  if (key == -2) {
                    throw new IllegalStateException("load error");
                  }
                  if (key == -3) {
                    throw new IOException("checked load error");
                  }
                  if (key == -4) {
                    throw new InterruptedException("interrupted load");
                  }
                  return "v" + key;
                });

    assertThat(cache.get(1)).isEqualTo("v1");
    assertThat(cache.get(1)).isEqualTo("v1");
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.get(-1)).isNull();
    assertThrows(IllegalStateException.class, () -> cache.get(-2));
    assertThrows(CompletionException.class, () -> cache.get(-3));
    assertThrows(CompletionException.class, () -> cache.get(-4));
    assertThat(Thread.interrupted()).isTrue();

    assertThat(cache.get(2, k -> "custom2")).isEqualTo("custom2");
    assertThat(cache.get(3, k -> null)).isNull();
    assertThat(cache.getAllPresent(Arrays.asList(1, 2, 99))).containsExactly(1, "v1", 2, "custom2");

    Map<Integer, String> allSingle = cache.getAll(Arrays.asList(1, 4, 5, -1));
    assertThat(allSingle).containsExactly(1, "v1", 4, "v4", 5, "v5");
    assertThat(cache.getAll(Collections.emptyList())).isEmpty();

    Map<Integer, String> fnAll =
        cache.getAll(
            Arrays.asList(1, 10, 11),
            keys -> {
              Map<Integer, String> m = new LinkedHashMap<>();
              m.put(10, "ten");
              m.put(12, "twelve");
              return m;
            });
    assertThat(fnAll).containsExactly(1, "v1", 10, "ten");
    assertThat(cache.getIfPresent(12)).isEqualTo("twelve");
    assertThrows(
        NullPointerException.class,
        () ->
            cache.getAll(
                Collections.singletonList(11), keys -> Collections.singletonMap(11, null)));

    // Bulk loading bounded cache
    LoadingCache<Integer, String> bulkBounded =
        Caffeine.newBuilder()
            .maximumSize(50)
            .executor(Runnable::run)
            .recordStats()
            .build(
                CacheLoader.bulk(
                    keys -> {
                      Map<Integer, String> res = new LinkedHashMap<>();
                      for (Integer k : keys) {
                        if (k == -9) {
                          throw new IllegalArgumentException("bad bulk");
                        }
                        if (k != -1) {
                          res.put(k, "b" + k);
                        }
                      }
                      res.put(500, "extra500");
                      return res;
                    }));
    assertThat(bulkBounded.getAll(Arrays.asList(1, 2, -1))).containsExactly(1, "b1", 2, "b2");
    assertThat(bulkBounded.getIfPresent(500)).isEqualTo("extra500");
    assertThrows(IllegalArgumentException.class, () -> bulkBounded.getAll(Arrays.asList(3, -9)));

    bulkBounded.invalidate(1);
    bulkBounded.invalidateAll(Arrays.asList(2, 500));
    bulkBounded.invalidateAll();
    assertThat(bulkBounded.estimatedSize()).isEqualTo(0L);
  }

  @Test
  public void concurrentMapAndExpirationEdgeCases() {
    FakeTicker ticker = new FakeTicker(1_000_000_000L);
    EnumMap<RemovalCause, AtomicInteger> causes = new EnumMap<>(RemovalCause.class);
    for (RemovalCause c : RemovalCause.values()) {
      causes.put(c, new AtomicInteger());
    }

    Cache<String, String> cache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(Duration.ofSeconds(10))
            .expireAfterAccess(Duration.ofSeconds(10))
            .ticker(ticker)
            .executor(Runnable::run)
            .removalListener((k, v, c) -> causes.get(c).incrementAndGet())
            .evictionListener((k, v, c) -> {})
            .recordStats()
            .build();
    ConcurrentMap<String, String> map = cache.asMap();

    assertThat(map).isExhaustivelyEmpty();
    assertThat(map.put("a", "1")).isNull();
    assertThat(map.put("b", "2")).isNull();
    assertThat(map.size()).isEqualTo(2);
    assertThat(map.containsKey("a")).isTrue();
    assertThat(map.containsValue("1")).isTrue();
    assertThat(map.containsValue("absent")).isFalse();
    assertThat(map.get("a")).isEqualTo("1");
    assertThat(map.getOrDefault("a", "d")).isEqualTo("1");
    assertThat(map.getOrDefault("z", "d")).isEqualTo("d");

    // Advance past expiration and verify map operations treat expired entries as absent and emit
    // EXPIRED
    ticker.advance(Duration.ofSeconds(15));
    assertThat(map.containsKey("a")).isFalse();
    assertThat(map.containsValue("1")).isFalse();
    assertThat(map.get("a")).isNull();
    assertThat(map.remove("b")).isNull();
    assertThat(causes.get(RemovalCause.EXPIRED).get()).isAtLeast(1);

    // Put on an expired entry replaces and notifies EXPIRED
    map.put("exp1", "v1");
    map.put("exp2", "v2");
    map.put("exp3", "v3");
    map.put("exp4", "v4");
    map.put("exp5", "v5");
    ticker.advance(Duration.ofSeconds(15));

    assertThat(map.put("exp1", "new1")).isNull();
    assertThat(map.putIfAbsent("exp2", "new2")).isNull();
    assertThat(map.replace("exp3", "new3")).isNull();
    assertThat(map.replace("exp4", "v4", "new4")).isFalse();
    assertThat(map.remove("exp5", "v5")).isFalse();

    cache.cleanUp();
    map.clear();

    // Standard ConcurrentMap operations on live entries
    map.put("k1", "v1");
    assertThat(map.put("k1", map.get("k1"))).isEqualTo("v1"); // same instance
    assertThat(map.put("k1", new String("v1"))).isEqualTo("v1"); // equal value -> REPLACED
    assertThat(map.put("k1", "v2")).isEqualTo("v1");

    assertThat(map.putIfAbsent("k1", "v3")).isEqualTo("v2");
    assertThat(map.putIfAbsent("k2", "v2")).isNull();

    assertThat(map.replace("missing", "x")).isNull();
    assertThat(map.replace("k2", "v22")).isEqualTo("v2");
    assertThat(map.replace("k2", "wrong", "v222")).isFalse();
    assertThat(map.replace("k2", "v22", "v222")).isTrue();
    map.replaceAll((k, v) -> v + "!");
    assertThat(map.get("k2")).isEqualTo("v222!");

    assertThat(map.computeIfAbsent("k1", k -> "no")).isEqualTo("v2!");
    assertThat(map.computeIfAbsent("k3", k -> null)).isNull();
    assertThat(map.computeIfAbsent("k3", k -> "v3")).isEqualTo("v3");

    assertThat(map.computeIfPresent("missing", (k, v) -> "x")).isNull();
    assertThat(map.computeIfPresent("k3", (k, v) -> "v33")).isEqualTo("v33");
    assertThat(map.computeIfPresent("k3", (k, v) -> null)).isNull();

    assertThat(map.compute("k4", (k, v) -> null)).isNull();
    assertThat(map.compute("k4", (k, v) -> "v4")).isEqualTo("v4");
    assertThat(map.compute("k4", (k, v) -> v + "4")).isEqualTo("v44");
    assertThat(map.compute("k4", (k, v) -> null)).isNull();

    assertThat(map.merge("k5", "m1", (o, n) -> o + n)).isEqualTo("m1");
    assertThat(map.merge("k5", "m2", (o, n) -> o + n)).isEqualTo("m1m2");
    assertThat(map.merge("k5", "m3", (o, n) -> null)).isNull();

    assertThat(map.remove("k1", null)).isFalse();
    assertThrows(NullPointerException.class, () -> map.remove(null, "v"));
    assertThat(map.remove("k1", "wrong")).isFalse();
    assertThat(map.remove("k1", "v2!")).isTrue();
    assertThat(map.remove("k2")).isEqualTo("v222!");

    map.put("p1", "1");
    map.put("p2", "2");
    Map<String, String> copy = new HashMap<>();
    map.forEach(copy::put);
    assertThat(copy).containsExactly("p1", "1", "p2", "2");
    assertThat(map.equals(copy)).isTrue();
    assertThat(map.hashCode()).isEqualTo(copy.hashCode());
    assertThat(map.toString()).contains("p1=1");
  }

  @Test
  public void viewsIteratorsAndSpliterators() {
    FakeTicker ticker = new FakeTicker(10_000_000L);
    Cache<Integer, String> cache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(Duration.ofSeconds(10))
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
    ConcurrentMap<Integer, String> map = cache.asMap();
    for (int i = 0; i < 20; i++) {
      map.put(i, "val-" + i);
    }

    // Spliterators: KeySpliterator, ValueSpliterator, EntrySpliterator (trySplit, tryAdvance,
    // forEachRemaining)
    Spliterator<Integer> keySplit1 = map.keySet().spliterator();
    assertThat(keySplit1.estimateSize()).isAtLeast(0L);
    assertThat(keySplit1.hasCharacteristics(Spliterator.DISTINCT | Spliterator.NONNULL)).isTrue();
    Spliterator<Integer> keySplit2 = keySplit1.trySplit();
    Set<Integer> splitKeys = new HashSet<>();
    assertThrows(NullPointerException.class, () -> keySplit1.tryAdvance(null));
    assertThrows(NullPointerException.class, () -> keySplit1.forEachRemaining(null));
    while (keySplit1.tryAdvance(splitKeys::add)) {
      // consume one by one via tryAdvance
    }
    if (keySplit2 != null) {
      keySplit2.forEachRemaining(splitKeys::add);
    }
    assertThat(splitKeys).hasSize(20);

    Spliterator<String> valSplit1 = map.values().spliterator();
    assertThat(valSplit1.estimateSize()).isAtLeast(0L);
    assertThat(valSplit1.hasCharacteristics(Spliterator.NONNULL)).isTrue();
    Spliterator<String> valSplit2 = valSplit1.trySplit();
    List<String> splitVals = new ArrayList<>();
    assertThrows(NullPointerException.class, () -> valSplit1.tryAdvance(null));
    assertThrows(NullPointerException.class, () -> valSplit1.forEachRemaining(null));
    while (valSplit1.tryAdvance(splitVals::add)) {
      // consume via tryAdvance
    }
    if (valSplit2 != null) {
      valSplit2.forEachRemaining(splitVals::add);
    }
    assertThat(splitVals).hasSize(20);

    Spliterator<Map.Entry<Integer, String>> entrySplit1 = map.entrySet().spliterator();
    assertThat(entrySplit1.estimateSize()).isAtLeast(0L);
    assertThat(entrySplit1.hasCharacteristics(Spliterator.DISTINCT | Spliterator.NONNULL)).isTrue();
    Spliterator<Map.Entry<Integer, String>> entrySplit2 = entrySplit1.trySplit();
    List<Map.Entry<Integer, String>> splitEntries = new ArrayList<>();
    assertThrows(NullPointerException.class, () -> entrySplit1.tryAdvance(null));
    assertThrows(NullPointerException.class, () -> entrySplit1.forEachRemaining(null));
    while (entrySplit1.tryAdvance(splitEntries::add)) {
      // consume via tryAdvance
    }
    if (entrySplit2 != null) {
      entrySplit2.forEachRemaining(splitEntries::add);
    }
    assertThat(splitEntries).hasSize(20);

    // Exhausted trySplit returns null
    assertThat(entrySplit1.trySplit()).isNull();
    assertThat(keySplit1.trySplit()).isNull();
    assertThat(valSplit1.trySplit()).isNull();

    // Expired entries skipped by iterators and spliterators
    ticker.advance(Duration.ofSeconds(5));
    map.put(100, "fresh-100");
    map.put(101, "fresh-101");
    ticker.advance(Duration.ofSeconds(6)); // 0..19 expired, 100..101 alive!

    Set<Integer> liveFromIterator = new HashSet<>();
    for (Integer k : map.keySet()) {
      liveFromIterator.add(k);
    }
    assertThat(liveFromIterator).containsExactly(100, 101);

    List<String> liveFromSpliterator = new ArrayList<>();
    map.values().spliterator().forEachRemaining(liveFromSpliterator::add);
    assertThat(liveFromSpliterator).containsExactly("fresh-100", "fresh-101");

    // Views operations on BoundedLocalCache
    Set<Integer> keySet = map.keySet();
    assertThat(keySet.contains(100)).isTrue();
    assertThat(keySet.contains("wrong")).isFalse();
    assertThat(keySet.remove("wrong")).isFalse();
    Iterator<Integer> kIt = keySet.iterator();
    assertThrows(IllegalStateException.class, kIt::remove);
    Integer kNext = kIt.next();
    kIt.remove();
    assertThrows(IllegalStateException.class, kIt::remove);
    assertThat(map.containsKey(kNext)).isFalse();
    while (kIt.hasNext()) {
      kIt.next();
    }
    assertThrows(NoSuchElementException.class, kIt::next);
    keySet.clear();
    assertThat(keySet).isEmpty();

    map.put(1, "one");
    map.put(2, "two");
    Collection<String> values = map.values();
    assertThat(values.contains("one")).isTrue();
    assertThat(values.remove(null)).isFalse();
    assertThat(values.remove("one")).isTrue();
    assertThat(values.remove("missing")).isFalse();
    Iterator<String> vIt = values.iterator();
    assertThat(vIt.next()).isEqualTo("two");
    vIt.remove();
    values.clear();

    map.put(1, "one");
    map.put(2, "two");
    Set<Map.Entry<Integer, String>> entries = map.entrySet();
    assertThrows(UnsupportedOperationException.class, () -> entries.add(new SimpleEntry<>(3, "3")));
    assertThat(entries.contains("wrong")).isFalse();
    assertThat(entries.contains(new SimpleEntry<>(1, "one"))).isTrue();
    assertThat(entries.contains(new SimpleEntry<>(1, "wrong"))).isFalse();
    assertThat(entries.remove("wrong")).isFalse();
    assertThat(entries.remove(new SimpleEntry<>(1, "wrong"))).isFalse();
    assertThat(entries.remove(new SimpleEntry<>(1, "one"))).isTrue();

    Iterator<Map.Entry<Integer, String>> eIt = entries.iterator();
    Map.Entry<Integer, String> wtEntry = eIt.next();
    assertThat(wtEntry.setValue("two-updated")).isEqualTo("two");
    assertThat(wtEntry.equals(wtEntry)).isTrue();
    assertThat(wtEntry.equals(new SimpleEntry<>(2, "two-updated"))).isTrue();
    assertThat(wtEntry.equals("other")).isFalse();
    assertThat(wtEntry.hashCode()).isEqualTo(new SimpleEntry<>(2, "two-updated").hashCode());
    assertThat(wtEntry.toString()).isEqualTo("2=two-updated");
    eIt.remove();
    assertThat(entries).isEmpty();
  }

  @Test
  public void boundedPolicyAndSnapshotEntries() {
    FakeTicker ticker = new FakeTicker(1_000_000_000L);

    // 1. Plain unweighted non-expiring BoundedPolicy + SnapshotEntry
    Cache<Integer, String> plain =
        Caffeine.newBuilder().maximumSize(10).executor(Runnable::run).build();
    plain.put(1, "a");
    plain.put(2, "b");
    plain.put(3, "c");
    plain.getIfPresent(1);
    plain.cleanUp();

    assertThat(plain.policy().isRecordingStats()).isFalse();
    assertThat(plain.policy().expireAfterAccess()).isEmpty();
    assertThat(plain.policy().expireAfterWrite()).isEmpty();
    assertThat(plain.policy().getIfPresentQuietly(1)).isEqualTo("a");
    assertThat(plain.policy().getIfPresentQuietly(99)).isNull();

    Policy.CacheEntry<Integer, String> plainEntry = plain.policy().getEntryIfPresentQuietly(1);
    assertThat(plainEntry).isNotNull();
    assertThat(plainEntry.getKey()).isEqualTo(1);
    assertThat(plainEntry.getValue()).isEqualTo("a");
    assertThat(plainEntry.weight()).isEqualTo(1);
    assertThat(plainEntry.expiresAt()).isEqualTo(Long.MAX_VALUE);
    assertThat(plainEntry.expiresAfter().toNanos()).isGreaterThan(0L);
    assertThrows(UnsupportedOperationException.class, () -> plainEntry.setValue("x"));
    assertThat(plainEntry.equals(plainEntry)).isTrue();
    assertThat(plainEntry.equals(new SimpleEntry<>(1, "a"))).isTrue();
    assertThat(plainEntry.equals("not-entry")).isFalse();
    assertThat(plainEntry.hashCode()).isEqualTo(new SimpleEntry<>(1, "a").hashCode());
    assertThat(plainEntry.toString()).isEqualTo("1=a");

    Policy.Eviction<Integer, String> plainEvict = plain.policy().eviction().orElseThrow();
    assertThat(plainEvict.isWeighted()).isFalse();
    assertThat(plainEvict.weightOf(1)).isEmpty();
    assertThat(plainEvict.weightedSize()).isEmpty();
    assertThat(plainEvict.getMaximum()).isEqualTo(10L);
    assertThat(plainEvict.coldest(2)).hasSize(2);
    assertThat(plainEvict.coldest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> plainEvict.coldest(-1));
    assertThat(plainEvict.hottest(2)).hasSize(2);
    assertThat(plainEvict.hottest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> plainEvict.hottest(-1));
    assertThat(plainEvict.coldestWeighted(2)).hasSize(2);
    assertThat(plainEvict.hottestWeighted(2)).hasSize(2);

    // 2. Weighted + ExpireAfterAccess + ExpireAfterWrite BoundedPolicy + ExpirableWeightedEntry
    Cache<Integer, Integer> full =
        Caffeine.newBuilder()
            .maximumWeight(50)
            .weigher((Integer k, Integer v) -> v)
            .expireAfterAccess(Duration.ofSeconds(20))
            .expireAfterWrite(Duration.ofSeconds(30))
            .ticker(ticker)
            .executor(Runnable::run)
            .recordStats()
            .build();
    full.put(1, 5);
    ticker.advance(Duration.ofSeconds(2));
    full.put(2, 10);
    ticker.advance(Duration.ofSeconds(2));
    full.put(3, 15);
    full.cleanUp();

    assertThat(full.policy().isRecordingStats()).isTrue();
    Policy.CacheEntry<Integer, Integer> ewEntry = full.policy().getEntryIfPresentQuietly(2);
    assertThat(ewEntry).isNotNull();
    assertThat(ewEntry.weight()).isEqualTo(10);
    assertThat(ewEntry.expiresAt()).isGreaterThan(ticker.read());
    assertThat(ewEntry.expiresAfter()).isEqualTo(Duration.ofSeconds(18));

    Policy.Eviction<Integer, Integer> weightedEvict = full.policy().eviction().orElseThrow();
    assertThat(weightedEvict.isWeighted()).isTrue();
    assertThat(weightedEvict.weightOf(2)).hasValue(10);
    assertThat(weightedEvict.weightOf(999)).isEmpty();
    assertThat(weightedEvict.weightedSize()).hasValue(30L);
    assertThat(weightedEvict.coldestWeighted(15)).containsKey(1);
    assertThat(weightedEvict.coldestWeighted(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> weightedEvict.coldestWeighted(-1));
    assertThat(weightedEvict.hottestWeighted(25)).containsKey(3);
    assertThat(weightedEvict.hottestWeighted(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> weightedEvict.hottestWeighted(-1));
    assertThrows(IllegalArgumentException.class, () -> weightedEvict.setMaximum(-1));
    weightedEvict.setMaximum(20);
    assertThat(weightedEvict.getMaximum()).isEqualTo(20L);
    assertThat(weightedEvict.weightedSize().getAsLong()).isAtMost(20L);

    Policy.FixedExpiration<Integer, Integer> accessPol =
        full.policy().expireAfterAccess().orElseThrow();
    assertThat(accessPol.getExpiresAfter()).isEqualTo(Duration.ofSeconds(20));
    assertThat(accessPol.ageOf(3, TimeUnit.SECONDS)).isPresent();
    assertThat(accessPol.ageOf(3)).isPresent();
    assertThat(accessPol.ageOf(999)).isEmpty();
    assertThat(accessPol.oldest(2)).isNotNull();
    assertThat(accessPol.oldest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> accessPol.oldest(-1));
    assertThat(accessPol.youngest(2)).isNotNull();
    assertThat(accessPol.youngest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> accessPol.youngest(-1));
    assertThrows(
        IllegalArgumentException.class, () -> accessPol.setExpiresAfter(-1, TimeUnit.SECONDS));
    accessPol.setExpiresAfter(Duration.ofSeconds(25));
    assertThat(accessPol.getExpiresAfter()).isEqualTo(Duration.ofSeconds(25));

    Policy.FixedExpiration<Integer, Integer> writePol =
        full.policy().expireAfterWrite().orElseThrow();
    assertThat(writePol.getExpiresAfter()).isEqualTo(Duration.ofSeconds(30));
    assertThat(writePol.ageOf(3, TimeUnit.SECONDS)).isPresent();
    assertThat(writePol.ageOf(999)).isEmpty();
    assertThat(writePol.oldest(2)).isNotNull();
    assertThat(writePol.oldest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> writePol.oldest(-1));
    assertThat(writePol.youngest(2)).isNotNull();
    assertThat(writePol.youngest(0)).isEmpty();
    assertThrows(IllegalArgumentException.class, () -> writePol.youngest(-1));
    assertThrows(
        IllegalArgumentException.class, () -> writePol.setExpiresAfter(-1, TimeUnit.SECONDS));
    writePol.setExpiresAfter(Duration.ofSeconds(1));
    ticker.advance(Duration.ofSeconds(5));
    assertThat(full.policy().getIfPresentQuietly(3)).isNull();
    assertThat(full.policy().getEntryIfPresentQuietly(3)).isNull();
    assertThat(writePol.ageOf(3)).isEmpty();
    assertThat(accessPol.ageOf(3)).isEmpty();

    // 3. Weighted-only (WeightedEntry) and Expirable-only (ExpirableEntry)
    Cache<Integer, Integer> weightedOnly =
        Caffeine.newBuilder()
            .maximumWeight(20)
            .weigher((Integer k, Integer v) -> v)
            .executor(Runnable::run)
            .build();
    weightedOnly.put(1, 7);
    assertThat(weightedOnly.policy().getEntryIfPresentQuietly(1).weight()).isEqualTo(7);

    Cache<Integer, Integer> expirableOnly =
        Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(10))
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
    expirableOnly.put(1, 7);
    assertThat(expirableOnly.policy().getEntryIfPresentQuietly(1).expiresAt())
        .isEqualTo(ticker.read() + Duration.ofSeconds(10).toNanos());
  }

  @Test
  public void wTinyLfuPromotionDemotionWeightUpdatesAndAdapt() {
    EnumMap<RemovalCause, AtomicInteger> evictedCauses = new EnumMap<>(RemovalCause.class);
    for (RemovalCause c : RemovalCause.values()) {
      evictedCauses.put(c, new AtomicInteger());
    }

    Cache<Integer, Integer> cache =
        Caffeine.newBuilder()
            .maximumWeight(20)
            .weigher((Integer k, Integer v) -> v)
            .executor(Runnable::run)
            .evictionListener((k, v, c) -> evictedCauses.get(c).incrementAndGet())
            .removalListener((k, v, c) -> {})
            .recordStats()
            .build();

    // Zero-weight entry is never evicted by size
    cache.put(0, 0);
    // Overweight entry is evicted immediately
    cache.put(999, 100);
    assertThat(cache.getIfPresent(999)).isNull();
    assertThat(evictedCauses.get(RemovalCause.SIZE).get()).isEqualTo(1);

    // Populate and access repeatedly to promote from Window -> Probation -> Protected, then demote
    // Protected
    for (int round = 0; round < 5; round++) {
      for (int i = 1; i <= 30; i++) {
        cache.put(i, 1);
        if (i <= 15) {
          cache.getIfPresent(i);
          cache.getIfPresent(i);
        }
      }
    }
    assertThat(cache.policy().eviction().orElseThrow().weightedSize().getAsLong()).isAtMost(20L);
    assertThat(cache.getIfPresent(0)).isEqualTo(0);

    // Update weights of existing entries in Window, Probation, Protected (including 0 -> positive
    // -> 0)
    cache.put(0, 5);
    cache.put(0, 0);
    for (Integer key : new ArrayList<>(cache.asMap().keySet())) {
      if (key != 0) {
        cache.put(key, 3);
      }
    }
    assertThat(cache.policy().eviction().orElseThrow().weightedSize().getAsLong()).isAtMost(20L);

    // Dynamic maximum resize: grow past DENSITY_THRESHOLD, run workload to trigger WindowClimber
    // step, then shrink to 0
    Policy.Eviction<Integer, Integer> eviction = cache.policy().eviction().orElseThrow();
    eviction.setMaximum(2500);
    for (int i = 0; i < 6000; i++) {
      int k = i % 3000;
      if (cache.getIfPresent(k) == null) {
        cache.put(k, 1);
      }
    }
    eviction.setMaximum(10);
    assertThat(eviction.weightedSize().getAsLong()).isAtMost(10L);
    eviction.setMaximum(0);
    assertThat(eviction.weightedSize().getAsLong()).isEqualTo(0L);
  }

  @Test
  public void drainStatusMaintenanceAndConcurrency() {
    // 1. Drain status state machine transitions
    AtomicBoolean scheduled = new AtomicBoolean();
    @SuppressWarnings({"unchecked", "rawtypes"})
    BoundedLocalCache<Integer, Integer> stateCache =
        new BoundedLocalCache<Integer, Integer>((Caffeine) Caffeine.newBuilder(), null, false) {
          @Override
          void scheduleDrainBuffers() {
            scheduled.set(true);
          }
        };
    Map<Integer, Integer> afterWriteTransitions =
        Map.of(
            IDLE, REQUIRED,
            REQUIRED, REQUIRED,
            PROCESSING_TO_IDLE, PROCESSING_TO_REQUIRED,
            PROCESSING_TO_REQUIRED, PROCESSING_TO_REQUIRED);
    afterWriteTransitions.forEach(
        (start, end) -> {
          stateCache.drainStatus = start;
          stateCache.scheduleAfterWrite();
          assertThat(stateCache.drainStatus).isEqualTo(end);
        });

    stateCache.drainStatus = 99999;
    assertThrows(IllegalStateException.class, stateCache::scheduleAfterWrite);

    Executor mockExecutor = Mockito.mock(Executor.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    BoundedLocalCache<Integer, Integer> drainCache =
        new BoundedLocalCache<Integer, Integer>(
            (Caffeine) Caffeine.newBuilder().executor(mockExecutor), null, false) {};
    assertThat(drainCache.drainBuffersTask).isNotInstanceOf(ForkJoinTask.class);
    Map<Integer, Integer> drainTransitions =
        Map.of(
            IDLE, PROCESSING_TO_IDLE,
            REQUIRED, PROCESSING_TO_IDLE,
            PROCESSING_TO_IDLE, PROCESSING_TO_IDLE,
            PROCESSING_TO_REQUIRED, PROCESSING_TO_REQUIRED);
    drainTransitions.forEach(
        (start, end) -> {
          drainCache.drainStatus = start;
          drainCache.scheduleDrainBuffers();
          assertThat(drainCache.drainStatus).isEqualTo(end);
          if (!start.equals(end)) {
            verify(mockExecutor).execute(any());
            reset(mockExecutor);
          }
        });

    // 2. PerformCleanupTask safety & exception catching
    @SuppressWarnings("unchecked")
    BoundedLocalCache<Object, Object> mockedCache = Mockito.mock(BoundedLocalCache.class);
    PerformCleanupTask task = new PerformCleanupTask(mockedCache);
    task.clear();
    assertThat(task.enqueue()).isFalse();
    assertThat(task.get()).isSameInstanceAs(mockedCache);
    task.run();
    verify(mockedCache).performCleanUp(any());

    doThrow(new RuntimeException("cleanup fail")).when(mockedCache).performCleanUp(any());
    task.run();
    doCallRealMethod().when(mockedCache).cleanUp();
    mockedCache.cleanUp();

    // 3. Reentrant listener during expireAfterAccess / expireAfterWrite / clear
    FakeTicker ticker = new FakeTicker(1_000_000L);
    AtomicReference<Cache<Integer, Integer>> selfRef = new AtomicReference<>();
    AtomicBoolean reentered = new AtomicBoolean();
    Cache<Integer, Integer> reentrantCache =
        Caffeine.newBuilder()
            .maximumSize(20)
            .expireAfterAccess(Duration.ofMinutes(1))
            .expireAfterWrite(Duration.ofMinutes(1))
            .ticker(ticker)
            .executor(Runnable::run)
            .scheduler(Scheduler.disabledScheduler())
            .removalListener(
                (Integer k, Integer v, RemovalCause c) -> {
                  if (reentered.compareAndSet(false, true)) {
                    Cache<Integer, Integer> self = selfRef.get();
                    self.invalidate(2);
                    self.cleanUp();
                  }
                })
            .build();
    selfRef.set(reentrantCache);
    for (int i = 1; i <= 6; i++) {
      reentrantCache.put(i, i);
    }
    ticker.advance(Duration.ofMinutes(2));
    reentrantCache.cleanUp();
    assertThat(reentrantCache.estimatedSize()).isEqualTo(0L);

    // 4. Scheduler rescheduling on incomplete clear
    Scheduler mockScheduler = Mockito.mock(Scheduler.class);
    when(mockScheduler.schedule(any(), any(), anyLong(), any()))
        .thenReturn(new CompletableFuture<>());
    Cache<Integer, Integer> pacedCache =
        Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(1))
            .scheduler(mockScheduler)
            .executor(Runnable::run)
            .build();
    BoundedLocalCache<Integer, Integer> blc =
        ((BoundedLocalCache.BoundedLocalManualCache<Integer, Integer>) pacedCache).cache;
    reset(mockScheduler);
    when(mockScheduler.schedule(any(), any(), anyLong(), any()))
        .thenReturn(new CompletableFuture<>());
    blc.drainStatus = REQUIRED;
    blc.clear();
    assertThat(blc.pacer().isScheduled()).isTrue();

    // 5. Listener failures (evictionListener throwing & InterruptedException, executor rejecting)
    Cache<Integer, Integer> failingListeners =
        Caffeine.newBuilder()
            .maximumSize(1)
            .executor(Runnable::run)
            .evictionListener(
                (k, v, c) -> {
                  throw new RejectedExecutionException("evict fail");
                })
            .removalListener(
                (k, v, c) -> {
                  throw new IllegalStateException("remove fail");
                })
            .build();
    failingListeners.put(1, 1);
    failingListeners.put(2, 2);
    failingListeners.invalidateAll();

    // 6. Multi-threaded stress on BoundedLocalCache (concurrent reads, writes, computes, evictions)
    Cache<Integer, Integer> concurrentCache =
        Caffeine.newBuilder().maximumSize(64).recordStats().build();
    ConcurrentTestHarness.timeTasks(
        8,
        () -> {
          for (int i = 0; i < 500; i++) {
            int key = i % 128;
            concurrentCache.get(key, k -> k * 2);
            if ((i & 7) == 0) {
              concurrentCache.put(key, i);
            }
            if ((i & 31) == 0) {
              concurrentCache.invalidate(key);
            }
          }
        });
    concurrentCache.cleanUp();
    assertThat(concurrentCache.estimatedSize()).isAtMost(64L);
    assertThat(concurrentCache.stats().requestCount()).isGreaterThan(0L);
  }

  @Test
  public void allFactoryCombinationsWithInitialCapacityAndScheduler() {
    FakeTicker ticker = new FakeTicker(1_000_000_000L);
    boolean[] bools = {false, true};

    for (boolean initCap : bools) {
      for (boolean maxSize : bools) {
        for (boolean weighted : bools) {
          if (maxSize && weighted) {
            continue;
          }
          for (boolean expAccess : bools) {
            for (boolean expWrite : bools) {
              for (boolean evictLis : bools) {
                for (boolean remLis : bools) {
                  for (boolean stats : bools) {
                    boolean hasExpiration = expAccess || expWrite;
                    boolean[] schedOptions = hasExpiration ? bools : new boolean[] {false};
                    for (boolean withSched : schedOptions) {
                      Caffeine<Object, Object> b = Caffeine.newBuilder().executor(Runnable::run);
                      if (initCap) {
                        b.initialCapacity(8);
                      }
                      if (maxSize) {
                        b.maximumSize(4);
                      }
                      if (weighted) {
                        b.maximumWeight(10).weigher((k, v) -> 3);
                      }
                      if (expAccess) {
                        b.expireAfterAccess(10, TimeUnit.SECONDS);
                      }
                      if (expWrite) {
                        b.expireAfterWrite(20, TimeUnit.SECONDS);
                      }
                      if (hasExpiration) {
                        b.ticker(ticker);
                      }
                      if (evictLis) {
                        b.evictionListener((k, v, c) -> {});
                      }
                      if (remLis) {
                        b.removalListener((k, v, c) -> {});
                      }
                      if (stats) {
                        b.recordStats();
                      }
                      if (withSched) {
                        b.scheduler(Scheduler.disabledScheduler());
                      }

                      Cache<String, Integer> cache = b.build();
                      for (int i = 0; i < 6; i++) {
                        cache.put("k" + i, i);
                      }
                      cache.cleanUp();
                      assertThat(cache.estimatedSize()).isAtLeast(1L);
                      if (cache instanceof BoundedLocalCache.BoundedLocalManualCache) {
                        BoundedLocalCache<String, Integer> inner =
                            ((BoundedLocalCache.BoundedLocalManualCache<String, Integer>) cache)
                                .cache;
                        for (Node<String, Integer> node : inner.data.values()) {
                          assertThat(node.toString()).contains("k");
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void bulkViewOperationsWindowTransferExpirationDelayAndInternals() {
    FakeTicker ticker = new FakeTicker(5_000_000_000L);
    Cache<Integer, String> cache =
        Caffeine.newBuilder()
            .maximumSize(32)
            .expireAfterAccess(Duration.ofSeconds(30))
            .expireAfterWrite(Duration.ofSeconds(60))
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
    BoundedLocalCache<Integer, String> blc =
        ((BoundedLocalCache.BoundedLocalManualCache<Integer, String>) cache).cache;
    ConcurrentMap<Integer, String> map = cache.asMap();

    for (int i = 1; i <= 8; i++) {
      map.put(i, "v" + i);
    }
    cache.cleanUp();

    // KeySetView, ValuesView, EntrySetView bulk methods (containsAll, removeAll, retainAll,
    // removeIf)
    Set<Integer> keys = map.keySet();
    assertThat(keys.containsAll(Arrays.asList(1, 2))).isTrue();
    assertThat(keys.containsAll(Arrays.asList(1, 999))).isFalse();
    assertThat(keys.removeIf(k -> k == 999)).isFalse();
    assertThat(keys.removeIf(k -> k == 8)).isTrue();
    assertThat(keys.removeAll(Collections.singletonList(999))).isFalse();
    assertThat(keys.removeAll(Collections.singletonList(7))).isTrue(); // c.size() <= size()
    assertThat(keys.removeAll(Arrays.asList(6, 100, 101, 102, 103, 104, 105, 106))).isTrue();
    assertThat(keys.retainAll(Arrays.asList(1, 2, 3, 4, 5))).isFalse();
    assertThat(keys.retainAll(Arrays.asList(1, 2, 3, 4))).isTrue();

    Collection<String> values = map.values();
    assertThat(values.containsAll(Arrays.asList("v1", "v2"))).isTrue();
    assertThat(values.containsAll(Arrays.asList("v1", "v999"))).isFalse();
    assertThat(values.removeIf(v -> v.equals("missing"))).isFalse();
    assertThat(values.removeIf(v -> v.equals("v4"))).isTrue();
    assertThat(values.removeAll(Collections.singletonList("missing"))).isFalse();
    assertThat(values.removeAll(Collections.singletonList("v3"))).isTrue();
    assertThat(values.retainAll(Arrays.asList("v1", "v2"))).isFalse();
    assertThat(values.retainAll(Collections.singletonList("v1"))).isTrue();

    map.put(2, "v2");
    map.put(3, "v3");
    map.put(4, "v4");
    Set<Map.Entry<Integer, String>> entries = map.entrySet();
    assertThat(entries.containsAll(Collections.singletonList(new SimpleEntry<>(1, "v1")))).isTrue();
    assertThat(entries.containsAll(Collections.singletonList(new SimpleEntry<>(1, "bad"))))
        .isFalse();
    assertThat(entries.removeIf(e -> e.getKey() == 999)).isFalse();
    assertThat(entries.removeIf(e -> e.getKey() == 4)).isTrue();
    assertThat(entries.removeAll(Collections.singletonList(new SimpleEntry<>(999, "x")))).isFalse();
    assertThat(entries.removeAll(Collections.singletonList(new SimpleEntry<>(3, "v3")))).isTrue();
    assertThat(
            entries.removeAll(
                Arrays.asList(
                    new SimpleEntry<>(2, "v2"),
                    new SimpleEntry<>(50, "x"),
                    new SimpleEntry<>(51, "x"),
                    new SimpleEntry<>(52, "x"))))
        .isTrue();
    assertThat(entries.retainAll(Collections.singletonList(new SimpleEntry<>(1, "v1")))).isFalse();
    assertThat(entries.retainAll(Collections.emptyList())).isTrue();
    assertThat(map).isEmpty();

    // BoundedLocalCache.equals edge cases (non-map, diff size, missing key, diff value, self)
    map.put(1, "a");
    map.put(2, "b");
    assertThat(map.equals(map)).isTrue();
    assertThat(map.equals("not-a-map")).isFalse();
    assertThat(map.equals(Map.of(1, "a"))).isFalse();
    assertThat(map.equals(Map.of(1, "a", 3, "b"))).isFalse();
    assertThat(map.equals(Map.of(1, "a", 2, "wrong"))).isFalse();
    assertThat(map.equals(Map.of(1, "a", 2, "b"))).isTrue();

    // Stage entries across window, probation, and protected to exercise increaseWindow,
    // decreaseWindow,
    // getExpirationDelay, and isPendingEviction
    map.clear();
    blc.evictionLock.lock();
    try {
      blc.setMaximumSize(20);
      blc.setWindowMaximum(0);
      blc.increaseWindow(); // mainProtectedMaximum > 0 with empty queues
      blc.setMainProtectedMaximum(0);
      blc.increaseWindow(); // early return when mainProtectedMaximum == 0
      blc.setWindowMaximum(1);
      blc.decreaseWindow(); // early return when windowMaximum <= 1
    } finally {
      blc.evictionLock.unlock();
    }

    blc.evictionLock.lock();
    try {
      blc.setMaximumSize(20);
      blc.setWindowMaximum(10);
      blc.setMainProtectedMaximum(8);
    } finally {
      blc.evictionLock.unlock();
    }
    for (int i = 1; i <= 12; i++) {
      map.put(i, "val" + i);
      if (i <= 4) {
        cache.getIfPresent(i);
        cache.getIfPresent(i);
      }
    }
    cache.cleanUp();

    blc.evictionLock.lock();
    try {
      // Decrease window: transfers from window -> probation
      blc.climber().carryOver(-4);
      blc.decreaseWindow();

      // Promote a couple probation entries into protected so protected & probation are both
      // non-empty
      for (int i = 1; i <= 3; i++) {
        Node<Integer, String> p = blc.accessOrderProbationDeque().peekFirst();
        if (p != null) {
          blc.accessOrderProbationDeque().remove(p);
          blc.accessOrderProtectedDeque().offerLast(p);
          p.makeMainProtected();
          blc.setMainProtectedWeightedSize(blc.mainProtectedWeightedSize() + p.getPolicyWeight());
        }
      }

      // Verify getExpirationDelay inspects window, probation, protected, and writeOrderDeque
      long delay = blc.getExpirationDelay(ticker.read());
      assertThat(delay).isGreaterThan(0L);
      assertThat(delay).isAtMost(Duration.ofSeconds(30).toNanos());

      // Increase window: demotes protected -> probation and transfers probation/protected -> window
      blc.climber().carryOver(5);
      blc.increaseWindow();
    } finally {
      blc.evictionLock.unlock();
    }

    // isPendingEviction: absent, alive, expired, retired
    assertThat(blc.isPendingEviction(999)).isFalse();
    assertThat(blc.isPendingEviction(1)).isFalse();
    ticker.advance(Duration.ofSeconds(35));
    assertThat(blc.isPendingEviction(1)).isTrue();
    cache.cleanUp();

    // Policy interface default methods on a minimal custom Policy implementation
    Policy<String, String> minimalPolicy =
        new Policy<String, String>() {
          @Override
          public boolean isRecordingStats() {
            return false;
          }

          @Override
          public String getIfPresentQuietly(String key) {
            return null;
          }

          @Override
          public Optional<Eviction<String, String>> eviction() {
            return Optional.empty();
          }

          @Override
          public Optional<FixedExpiration<String, String>> expireAfterAccess() {
            return Optional.empty();
          }

          @Override
          public Optional<FixedExpiration<String, String>> expireAfterWrite() {
            return Optional.empty();
          }
        };
    assertThrows(
        UnsupportedOperationException.class, () -> minimalPolicy.getEntryIfPresentQuietly("k"));

    // Node default methods on base PS node
    Node<String, String> baseNode = new PS<String, String>().newNode("key", "val", null, 1, 0L);
    assertThat(baseNode.getKey()).isEqualTo("key");
    assertThat(baseNode.getValue()).isEqualTo("val");
    assertThat(baseNode.getWeight()).isEqualTo(1);
    baseNode.setWeight(5);
    assertThat(baseNode.getPolicyWeight()).isEqualTo(1);
    assertThat(baseNode.getAccessTime()).isEqualTo(0L);
    baseNode.setAccessTime(10L);
    assertThat(baseNode.getWriteTime()).isEqualTo(0L);
    baseNode.setWriteTime(10L);
    assertThrows(UnsupportedOperationException.class, () -> baseNode.casWriteTime(0L, 10L));
    assertThat(baseNode.getVariableTime()).isEqualTo(0L);
    baseNode.setVariableTime(10L);
    assertThrows(UnsupportedOperationException.class, () -> baseNode.casVariableTime(0L, 10L));
    assertThrows(UnsupportedOperationException.class, () -> baseNode.setQueueType(1));
    assertThat(baseNode.getPreviousInAccessOrder()).isNull();
    assertThat(baseNode.getNextInAccessOrder()).isNull();
    assertThrows(
        UnsupportedOperationException.class, () -> baseNode.setPreviousInAccessOrder(null));
    assertThrows(UnsupportedOperationException.class, () -> baseNode.setNextInAccessOrder(null));
    assertThat(baseNode.getPreviousInWriteOrder()).isNull();
    assertThat(baseNode.getNextInWriteOrder()).isNull();
    assertThrows(UnsupportedOperationException.class, () -> baseNode.setPreviousInWriteOrder(null));
    assertThrows(UnsupportedOperationException.class, () -> baseNode.setNextInWriteOrder(null));
    assertThrows(UnsupportedOperationException.class, baseNode::getPreviousInVariableOrder);
    assertThrows(UnsupportedOperationException.class, baseNode::getNextInVariableOrder);
    assertThrows(
        UnsupportedOperationException.class, () -> baseNode.setPreviousInVariableOrder(null));
    assertThrows(UnsupportedOperationException.class, () -> baseNode.setNextInVariableOrder(null));
    assertThat(baseNode.inWindow()).isTrue();
    assertThat(baseNode.inMainProbation()).isFalse();
    assertThat(baseNode.inMainProtected()).isFalse();
    assertThat(baseNode.toString()).contains("key=key");
  }
}
