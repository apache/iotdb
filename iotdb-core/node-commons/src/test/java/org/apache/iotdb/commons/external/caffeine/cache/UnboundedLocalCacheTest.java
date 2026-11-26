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

import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;

/** Comprehensive unit tests for {@link UnboundedLocalCache} and its loading and map views. */
public final class UnboundedLocalCacheTest {

  @Test
  public void policyAndBasicManualCache() {
    Cache<Integer, String> cache = Caffeine.newBuilder().recordStats().build();
    assertThat(cache.policy().isRecordingStats()).isTrue();
    assertThat(cache.policy().eviction()).isEmpty();
    assertThat(cache.policy().expireAfterAccess()).isEmpty();
    assertThat(cache.policy().expireAfterWrite()).isEmpty();
    assertThat(cache.policy().getIfPresentQuietly(1)).isNull();
    assertThat(cache.policy().getEntryIfPresentQuietly(1)).isNull();

    Cache<Integer, String> noStats = Caffeine.newBuilder().initialCapacity(4).build();
    assertThat(noStats.policy().isRecordingStats()).isFalse();

    cache.put(1, "one");
    cache.put(2, "two");
    assertThat(cache.policy().getIfPresentQuietly(1)).isEqualTo("one");
    Policy.CacheEntry<Integer, String> quietEntry = cache.policy().getEntryIfPresentQuietly(1);
    assertThat(quietEntry).isNotNull();
    assertThat(quietEntry.getKey()).isEqualTo(1);
    assertThat(quietEntry.getValue()).isEqualTo("one");
    assertThat(quietEntry.weight()).isEqualTo(1);

    assertThat(cache.getIfPresent(1)).isEqualTo("one");
    assertThat(cache.getIfPresent(99)).isNull();
    assertThat(cache.stats().hitCount()).isEqualTo(1L);
    assertThat(cache.stats().missCount()).isEqualTo(1L);

    assertThat(cache.get(1, k -> "other")).isEqualTo("one");
    assertThat(cache.get(3, k -> "three")).isEqualTo("three");
    assertThat(cache.get(4, k -> null)).isNull();
    assertThrows(
        IllegalStateException.class,
        () ->
            cache.get(
                5,
                k -> {
                  throw new IllegalStateException("boom");
                }));

    Map<Integer, String> present = cache.getAllPresent(Arrays.asList(1, 2, 99, 1));
    assertThat(present).hasSize(2);
    assertThat(present.get(1)).isEqualTo("one");
    assertThrows(UnsupportedOperationException.class, present::clear);

    Map<Integer, String> emptyAll = cache.getAll(Collections.emptyList(), keys -> Map.of());
    assertThat(emptyAll).isEmpty();

    Map<Integer, String> loadedAll =
        cache.getAll(
            Arrays.asList(1, 10, 11),
            keys -> {
              Map<Integer, String> res = new LinkedHashMap<>();
              res.put(10, "ten");
              res.put(12, "twelve-extra");
              return res;
            });
    assertThat(loadedAll).containsExactly(1, "one", 10, "ten");
    assertThat(cache.getIfPresent(12)).isEqualTo("twelve-extra");
    assertThrows(
        NullPointerException.class,
        () ->
            cache.getAll(
                Collections.singletonList(11), keys -> Collections.singletonMap(11, null)));

    assertThrows(
        NullPointerException.class, () -> cache.getAll(Arrays.asList(20, 21), keys -> null));
    assertThrows(
        NullPointerException.class,
        () -> cache.getAll(Arrays.asList(1, null), keys -> Collections.emptyMap()));

    Map<Integer, String> batch = new LinkedHashMap<>();
    batch.put(30, "thirty");
    batch.put(31, "thirty-one");
    cache.putAll(batch);
    assertThat(cache.estimatedSize()).isEqualTo(7L);

    cache.invalidate(30);
    assertThat(cache.getIfPresent(30)).isNull();
    cache.invalidateAll(Arrays.asList(31, 999));
    assertThat(cache.getIfPresent(31)).isNull();
    cache.cleanUp();
    cache.invalidateAll();
    assertThat(cache.estimatedSize()).isEqualTo(0L);
  }

  @Test
  public void loadingCacheSingleAndBulk() {
    AtomicInteger singleLoads = new AtomicInteger();
    LoadingCache<Integer, String> singleCache =
        Caffeine.newBuilder()
            .recordStats()
            .build(
                key -> {
                  singleLoads.incrementAndGet();
                  if (key == -1) {
                    return null;
                  }
                  if (key == -2) {
                    throw new IllegalArgumentException("bad key");
                  }
                  if (key == -3) {
                    throw new IOException("io fail");
                  }
                  if (key == -4) {
                    throw new InterruptedException("interrupted");
                  }
                  return "val-" + key;
                });

    assertThrows(NullPointerException.class, () -> singleCache.get(null));
    assertThat(singleCache.get(1)).isEqualTo("val-1");
    assertThat(singleCache.get(1)).isEqualTo("val-1");
    assertThat(singleLoads.get()).isEqualTo(1);
    assertThat(singleCache.get(-1)).isNull();
    assertThrows(IllegalArgumentException.class, () -> singleCache.get(-2));
    CompletionException ioWrapped =
        assertThrows(CompletionException.class, () -> singleCache.get(-3));
    assertThat(ioWrapped).hasCauseThat().isInstanceOf(IOException.class);
    CompletionException intWrapped =
        assertThrows(CompletionException.class, () -> singleCache.get(-4));
    assertThat(intWrapped).hasCauseThat().isInstanceOf(InterruptedException.class);
    assertThat(Thread.interrupted()).isTrue();

    Map<Integer, String> multiFromSingle = singleCache.getAll(Arrays.asList(1, 2, 3, -1));
    assertThat(multiFromSingle).containsExactly(1, "val-1", 2, "val-2", 3, "val-3");
    assertThat(singleCache.getAll(Collections.emptyList())).isEmpty();
    assertThrows(NullPointerException.class, () -> singleCache.getAll(Arrays.asList(1, null)));

    // BulkCacheLoader via CacheLoader.bulk(...)
    CacheLoader<Integer, String> bulkLoader =
        CacheLoader.bulk(
            keys -> {
              Map<Integer, String> out = new LinkedHashMap<>();
              for (Integer k : keys) {
                if (k == -10) {
                  throw new IllegalStateException("bulk error");
                }
                if (k != -1) {
                  out.put(k, "bulk-" + k);
                }
              }
              out.put(999, "extra");
              return out;
            });
    LoadingCache<Integer, String> bulkCache = Caffeine.newBuilder().recordStats().build(bulkLoader);
    assertThat(bulkCache.get(5)).isEqualTo("bulk-5");
    assertThat(bulkCache.get(-1)).isNull();
    Map<Integer, String> bulkRes = bulkCache.getAll(Arrays.asList(5, 6, 7, -1));
    assertThat(bulkRes).containsExactly(5, "bulk-5", 6, "bulk-6", 7, "bulk-7");
    assertThat(bulkCache.getIfPresent(999)).isEqualTo("extra");
    assertThrows(IllegalStateException.class, () -> bulkCache.getAll(Arrays.asList(8, -10)));

    // Checked exception in overridden loadAll
    LoadingCache<Integer, String> checkedBulkCache =
        Caffeine.newBuilder()
            .recordStats()
            .build(
                new CacheLoader<Integer, String>() {
                  @Override
                  public String load(Integer key) {
                    return "v" + key;
                  }

                  @Override
                  public Map<Integer, String> loadAll(Set<? extends Integer> keys)
                      throws Exception {
                    throw new IOException("bulk io");
                  }
                });
    assertThrows(CompletionException.class, () -> checkedBulkCache.getAll(Arrays.asList(1, 2)));

    // Immutable keys passed to loadAll
    LoadingCache<Integer, String> mutatingLoaderCache =
        Caffeine.newBuilder()
            .build(
                new CacheLoader<Integer, String>() {
                  @Override
                  public String load(Integer key) {
                    return "v";
                  }

                  @Override
                  public Map<Integer, String> loadAll(Set<? extends Integer> keys) {
                    keys.clear();
                    return Collections.emptyMap();
                  }
                });
    assertThrows(
        UnsupportedOperationException.class, () -> mutatingLoaderCache.getAll(Arrays.asList(1, 2)));
  }

  @Test
  public void concurrentMapAndRemovalNotifications() {
    EnumMap<RemovalCause, AtomicInteger> causes = new EnumMap<>(RemovalCause.class);
    for (RemovalCause c : RemovalCause.values()) {
      causes.put(c, new AtomicInteger());
    }
    Cache<String, String> cache =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .removalListener((k, v, cause) -> causes.get(cause).incrementAndGet())
            .build();
    ConcurrentMap<String, String> map = cache.asMap();

    assertThat(map).isExhaustivelyEmpty();
    assertThrows(NullPointerException.class, () -> map.containsKey(null));
    assertThrows(NullPointerException.class, () -> map.containsValue(null));
    assertThrows(NullPointerException.class, () -> map.get(null));
    assertThrows(NullPointerException.class, () -> map.getOrDefault(null, "d"));
    assertThrows(NullPointerException.class, () -> map.put(null, "v"));
    assertThrows(NullPointerException.class, () -> map.put("k", null));

    assertThat(map.getOrDefault("a", "def")).isEqualTo("def");
    assertThat(map.put("a", "1")).isNull();
    assertThat(map.getOrDefault("a", "def")).isEqualTo("1");
    assertThat(map.containsKey("a")).isTrue();
    assertThat(map.containsValue("1")).isTrue();
    assertThat(map.containsValue("nope")).isFalse();

    // Replacing with same instance does NOT fire REPLACED; equal new instance DOES fire REPLACED
    String sameInstance = map.get("a");
    assertThat(map.put("a", sameInstance)).isSameInstanceAs(sameInstance);
    assertThat(causes.get(RemovalCause.REPLACED).get()).isEqualTo(0);

    assertThat(map.put("a", new String("1"))).isEqualTo("1");
    assertThat(causes.get(RemovalCause.REPLACED).get()).isEqualTo(1);

    // putIfAbsent
    assertThat(map.putIfAbsent("a", "2")).isEqualTo("1");
    assertThat(map.putIfAbsent("b", "2")).isNull();

    // replace(k, v) and replace(k, old, new)
    assertThat(map.replace("missing", "x")).isNull();
    assertThat(map.replace("b", "2")).isEqualTo("2"); // same instance
    assertThat(map.replace("b", "22")).isEqualTo("2");
    assertThat(map.replace("b", "wrong", "222")).isFalse();
    assertThat(map.replace("missing", "22", "222")).isFalse();
    assertThat(map.replace("b", "22", "22")).isTrue(); // same instance
    assertThat(map.replace("b", "22", "222")).isTrue();

    // replaceAll
    map.replaceAll((k, v) -> k.equals("a") ? v : v + "!");
    assertThat(map.get("b")).isEqualTo("222!");

    // computeIfAbsent / computeIfPresent / compute / merge
    assertThat(map.computeIfAbsent("a", k -> "ignored")).isEqualTo("1");
    assertThat(map.computeIfAbsent("c", k -> null)).isNull();
    assertThat(map.computeIfAbsent("c", k -> "3")).isEqualTo("3");

    assertThat(map.computeIfPresent("missing", (k, v) -> "x")).isNull();
    assertThat(map.computeIfPresent("c", (k, v) -> "33")).isEqualTo("33");
    assertThat(map.computeIfPresent("c", (k, v) -> null)).isNull();
    assertThat(causes.get(RemovalCause.EXPLICIT).get()).isEqualTo(1);

    assertThat(map.compute("d", (k, v) -> v == null ? null : "x")).isNull();
    assertThat(map.compute("d", (k, v) -> "4")).isEqualTo("4");
    assertThat(map.compute("d", (k, v) -> v + "4")).isEqualTo("44");
    assertThat(map.compute("d", (k, v) -> null)).isNull();

    assertThat(map.merge("e", "5", (o, n) -> o + n)).isEqualTo("5");
    assertThat(map.merge("e", "5", (o, n) -> o + n)).isEqualTo("55");
    assertThat(map.merge("e", "5", (o, n) -> null)).isNull();

    // remove(k) and remove(k, v)
    assertThat(map.remove("missing")).isNull();
    assertThat(map.remove("a", null)).isFalse();
    assertThrows(NullPointerException.class, () -> map.remove(null, "1"));
    assertThat(map.remove("a", "wrong")).isFalse();
    assertThat(map.remove("a", "1")).isTrue();
    assertThat(map.remove("b")).isEqualTo("222!");

    map.put("x", "10");
    map.put("y", "20");
    Map<String, String> seen = new HashMap<>();
    map.forEach(seen::put);
    assertThat(seen).containsExactly("x", "10", "y", "20");

    // equals, hashCode, toString
    Map<String, String> expected = new HashMap<>();
    expected.put("x", "10");
    expected.put("y", "20");
    assertThat(map.equals(map)).isTrue();
    assertThat(map.equals(expected)).isTrue();
    assertThat(map.equals("not-a-map")).isFalse();
    assertThat(map.equals(Map.of("x", "10"))).isFalse();
    assertThat(map.equals(Map.of("x", "10", "y", "99"))).isFalse();
    assertThat(map.hashCode()).isEqualTo(expected.hashCode());
    assertThat(map.toString()).contains("x=10");

    map.clear();
    assertThat(map).isEmpty();
    assertThat(map.toString()).isEqualTo("{}");

    // Self-referential map toString
    Cache<Object, Object> selfCache = Caffeine.newBuilder().build();
    ConcurrentMap<Object, Object> selfMap = selfCache.asMap();
    selfMap.put(selfMap, selfMap);
    assertThat(selfMap.toString()).isEqualTo("{(this Map)=(this Map)}");
  }

  @Test
  public void collectionViewsAndIterators() {
    Cache<Integer, String> cache = Caffeine.newBuilder().executor(Runnable::run).build();
    ConcurrentMap<Integer, String> map = cache.asMap();
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");

    // KeySetView
    Set<Integer> keys = map.keySet();
    assertThat(keys.size()).isEqualTo(3);
    assertThat(keys.isEmpty()).isFalse();
    assertThat(keys.contains(1)).isTrue();
    assertThat(keys.contains(99)).isFalse();
    assertThat(keys.toArray()).hasLength(3);
    assertThat(keys.toArray(new Integer[0])).hasLength(3);
    assertThat(keys.equals(keys)).isTrue();
    assertThat(keys.equals(Set.of(1, 2, 3))).isTrue();
    assertThat(keys.equals("other")).isFalse();
    assertThat(keys.hashCode()).isEqualTo(Set.of(1, 2, 3).hashCode());
    assertThat(keys.toString()).contains("1");

    Iterator<Integer> keyIt = keys.iterator();
    assertThrows(IllegalStateException.class, keyIt::remove);
    assertThat(keyIt.hasNext()).isTrue();
    Integer removedKey = keyIt.next();
    keyIt.remove();
    assertThrows(IllegalStateException.class, keyIt::remove);
    assertThat(map.containsKey(removedKey)).isFalse();
    while (keyIt.hasNext()) {
      keyIt.next();
    }
    assertThrows(NoSuchElementException.class, keyIt::next);

    map.put(removedKey, "restored");
    assertThat(keys.remove(1)).isTrue();
    assertThat(keys.remove(1)).isFalse();
    assertThat(keys.remove("wrong-type")).isFalse();
    keys.clear();
    assertThat(keys.isEmpty()).isTrue();

    // ValuesView
    map.put(1, "a");
    map.put(2, "b");
    map.put(3, "c");
    Collection<String> values = map.values();
    assertThat(values.size()).isEqualTo(3);
    assertThat(values.isEmpty()).isFalse();
    assertThat(values.contains("a")).isTrue();
    assertThat(values.contains("z")).isFalse();
    assertThat(values.toArray()).hasLength(3);
    assertThat(values.toArray(new String[0])).hasLength(3);
    assertThat(values.toString()).contains("a");

    Iterator<String> valIt = values.iterator();
    assertThrows(IllegalStateException.class, valIt::remove);
    assertThat(valIt.next()).isNotNull();
    valIt.remove();
    assertThrows(IllegalStateException.class, valIt::remove);
    while (valIt.hasNext()) {
      valIt.next();
    }
    assertThrows(NoSuchElementException.class, valIt::next);

    assertThat(values.remove(null)).isFalse();
    String remainingVal = values.iterator().next();
    assertThat(values.remove(remainingVal)).isTrue();
    assertThat(values.remove("non-existent")).isFalse();
    assertThat(values.removeIf(v -> true)).isTrue();
    assertThat(values.removeIf(v -> true)).isFalse();
    map.put(10, "ten");
    values.clear();
    assertThat(values.isEmpty()).isTrue();

    // EntrySetView & WriteThroughEntry
    map.put(1, "a");
    map.put(2, "b");
    Set<Map.Entry<Integer, String>> entries = map.entrySet();
    assertThat(entries.size()).isEqualTo(2);
    assertThat(entries.isEmpty()).isFalse();
    assertThat(entries.contains("not-an-entry")).isFalse();
    assertThat(entries.contains(new SimpleEntry<>(null, "a"))).isFalse();
    assertThat(entries.contains(new SimpleEntry<>(1, "a"))).isTrue();
    assertThat(entries.contains(new SimpleEntry<>(1, "wrong"))).isFalse();
    assertThrows(UnsupportedOperationException.class, () -> entries.add(new SimpleEntry<>(3, "c")));

    Iterator<Map.Entry<Integer, String>> entryIt = entries.iterator();
    assertThrows(IllegalStateException.class, entryIt::remove);
    Map.Entry<Integer, String> entry = entryIt.next();
    assertThat(entry.equals(entry)).isTrue();
    assertThat(entry.equals(new SimpleEntry<>(entry.getKey(), entry.getValue()))).isTrue();
    assertThat(entry.equals("not-entry")).isFalse();
    assertThat(entry.hashCode())
        .isEqualTo(new SimpleEntry<>(entry.getKey(), entry.getValue()).hashCode());
    assertThat(entry.toString()).contains("=");
    assertThat(entry.setValue("updated")).isNotNull();
    assertThat(map.get(entry.getKey())).isEqualTo("updated");

    entryIt.remove();
    assertThrows(IllegalStateException.class, entryIt::remove);
    while (entryIt.hasNext()) {
      entryIt.next();
    }
    assertThrows(NoSuchElementException.class, entryIt::next);

    map.put(5, "five");
    map.put(6, "six");
    assertThat(entries.remove("not-entry")).isFalse();
    assertThat(entries.remove(new SimpleEntry<>(5, "wrong"))).isFalse();
    assertThat(entries.remove(new SimpleEntry<>(5, "five"))).isTrue();
    assertThat(entries.removeIf(e -> e.getKey() == 6)).isTrue();
    map.put(7, "seven");
    entries.clear();
    assertThat(entries.isEmpty()).isTrue();

    // Bulk collection methods (containsAll, removeAll with small/large collection, retainAll,
    // removeIf)
    // and Spliterators (KeySpliterator, ValueSpliterator, EntrySpliterator)
    for (int i = 1; i <= 6; i++) {
      map.put(i, "v" + i);
    }
    assertThat(keys.containsAll(Arrays.asList(1, 2))).isTrue();
    assertThat(keys.containsAll(Arrays.asList(1, 99))).isFalse();
    assertThat(keys.removeIf(k -> k == 99)).isFalse();
    assertThat(keys.removeIf(k -> k == 6)).isTrue();
    assertThat(keys.removeAll(Arrays.asList(98, 99))).isFalse();
    assertThat(keys.removeAll(Collections.singletonList(5))).isTrue(); // c.size() <= size()
    assertThat(keys.removeAll(Arrays.asList(4, 10, 11, 12, 13, 14))).isTrue(); // c.size() > size()
    assertThat(keys.retainAll(Arrays.asList(1, 2, 3))).isFalse();
    assertThat(keys.retainAll(Arrays.asList(1, 2))).isTrue();

    map.put(3, "v3");
    map.put(4, "v4");
    assertThat(values.containsAll(Arrays.asList("v1", "v2"))).isTrue();
    assertThat(values.containsAll(Arrays.asList("v1", "v99"))).isFalse();
    assertThat(values.removeAll(Collections.singletonList("missing"))).isFalse();
    assertThat(values.removeAll(Collections.singletonList("v4"))).isTrue();
    assertThat(values.retainAll(Arrays.asList("v1", "v2", "v3"))).isFalse();
    assertThat(values.retainAll(Arrays.asList("v1", "v2"))).isTrue();

    map.put(3, "v3");
    map.put(4, "v4");
    assertThat(entries.containsAll(Collections.singletonList(new SimpleEntry<>(1, "v1")))).isTrue();
    assertThat(entries.containsAll(Collections.singletonList(new SimpleEntry<>(1, "bad"))))
        .isFalse();
    assertThat(entries.removeIf(e -> e.getKey() == 99)).isFalse();
    assertThat(entries.removeAll(Collections.singletonList(new SimpleEntry<>(99, "x")))).isFalse();
    assertThat(entries.removeAll(Collections.singletonList(new SimpleEntry<>(4, "v4")))).isTrue();
    assertThat(
            entries.removeAll(
                Arrays.asList(
                    new SimpleEntry<>(3, "v3"),
                    new SimpleEntry<>(10, "x"),
                    new SimpleEntry<>(11, "x"),
                    new SimpleEntry<>(12, "x"))))
        .isTrue();
    assertThat(
            entries.retainAll(
                Arrays.asList(new SimpleEntry<>(1, "v1"), new SimpleEntry<>(2, "v2"))))
        .isFalse();
    assertThat(entries.retainAll(Collections.singletonList(new SimpleEntry<>(1, "v1")))).isTrue();

    for (int i = 1; i <= 16; i++) {
      map.put(i, "s" + i);
    }
    var ks1 = keys.spliterator();
    var ks2 = ks1.trySplit();
    List<Integer> splitKeys = new ArrayList<>();
    ks1.tryAdvance(splitKeys::add);
    ks1.forEachRemaining(splitKeys::add);
    if (ks2 != null) {
      ks2.forEachRemaining(splitKeys::add);
    }
    assertThat(splitKeys).hasSize(16);

    var vs1 = values.spliterator();
    var vs2 = vs1.trySplit();
    List<String> splitVals = new ArrayList<>();
    vs1.tryAdvance(splitVals::add);
    vs1.forEachRemaining(splitVals::add);
    if (vs2 != null) {
      vs2.forEachRemaining(splitVals::add);
    }
    assertThat(splitVals).hasSize(16);

    var es1 = entries.spliterator();
    var es2 = es1.trySplit();
    List<Map.Entry<Integer, String>> splitEntries = new ArrayList<>();
    es1.tryAdvance(splitEntries::add);
    es1.forEachRemaining(splitEntries::add);
    if (es2 != null) {
      es2.forEachRemaining(splitEntries::add);
    }
    assertThat(splitEntries).hasSize(16);
  }

  @Test
  public void listenerAndExecutorFailurePaths() {
    Cache<Integer, Integer> listenerThrows =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .removalListener(
                (k, v, c) -> {
                  throw new IllegalStateException("listener boom");
                })
            .build();
    listenerThrows.put(1, 1);
    listenerThrows.invalidate(1);
    assertThat(listenerThrows.estimatedSize()).isEqualTo(0L);

    Cache<Integer, Integer> executorRejects =
        Caffeine.newBuilder()
            .executor(
                task -> {
                  throw new RejectedExecutionException("rejected");
                })
            .removalListener((k, v, c) -> {})
            .build();
    executorRejects.put(1, 1);
    executorRejects.invalidate(1);
    assertThat(executorRejects.estimatedSize()).isEqualTo(0L);
  }
}
