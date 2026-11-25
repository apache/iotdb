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

import org.apache.iotdb.commons.external.caffeine.cache.stats.StatsCounter;
import org.apache.iotdb.commons.external.caffeine.cache.testing.FakeTicker;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;

/**
 * Ported unit tests for {@link Caffeine}, {@link Ticker}, {@link Weigher}, and {@link
 * RemovalCause}.
 */
public final class CaffeineTest {

  private static final CacheLoader<Object, Object> LOADER =
      key -> {
        throw new AssertionError();
      };

  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  @Test
  public void unconfigured() {
    assertThat(Caffeine.newBuilder().build()).isNotNull();
    assertThat(Caffeine.newBuilder().build(LOADER)).isNotNull();
    assertThat(Caffeine.newBuilder().toString()).isEqualTo(Caffeine.newBuilder().toString());
    assertThat(Caffeine.newBuilder().toString()).isEqualTo("Caffeine{}");
  }

  @Test
  public void configuredAndToString() {
    Caffeine<Object, Object> configured =
        Caffeine.newBuilder()
            .initialCapacity(16)
            .maximumSize(100)
            .expireAfterAccess(Duration.ofSeconds(1))
            .expireAfterWrite(Duration.ofSeconds(2))
            .evictionListener((k, v, c) -> {})
            .removalListener((k, v, c) -> {})
            .recordStats();
    assertThat(configured.build()).isNotNull();
    assertThat(configured.build(LOADER)).isNotNull();

    String desc = configured.toString();
    assertThat(desc).contains("initialCapacity=16");
    assertThat(desc).contains("maximumSize=100");
    assertThat(desc).contains("expireAfterWrite=2000000000ns");
    assertThat(desc).contains("expireAfterAccess=1000000000ns");
    assertThat(desc).contains("evictionListener");
    assertThat(desc).contains("removalListener");

    String weightedDesc = Caffeine.newBuilder().maximumWeight(50).weigher((k, v) -> 1).toString();
    assertThat(weightedDesc).contains("maximumWeight=50");
  }

  @Test
  public void calculateHashMapCapacity() {
    List<Integer> list = Arrays.asList(1, 2, 3);
    Iterable<Integer> iterable = list::iterator;
    assertThat(Caffeine.calculateHashMapCapacity(iterable)).isEqualTo(16);
    assertThat(Caffeine.calculateHashMapCapacity(list)).isEqualTo(4);
    assertThat(Caffeine.calculateHashMapCapacity(0)).isEqualTo(0);
  }

  @Test
  public void hasMethodOverride() {
    assertThat(Caffeine.hasMethodOverride(CacheLoader.class, LOADER, "loadAll", Set.class))
        .isFalse();
    assertThat(Caffeine.hasMethodOverride(CacheLoader.class, LOADER, "abc_xyz", Set.class))
        .isFalse();

    CacheLoader<Object, Object> overridingLoader =
        new CacheLoader<Object, Object>() {
          @Override
          public Object load(Object key) {
            throw new AssertionError();
          }

          @Override
          public Map<Object, Object> loadAll(Set<?> keys) {
            throw new AssertionError();
          }
        };
    assertThat(
            Caffeine.hasMethodOverride(CacheLoader.class, overridingLoader, "loadAll", Set.class))
        .isTrue();
  }

  @Test
  public void loadingNullLoader() {
    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().build(null));
  }

  @Test
  public void initialCapacity() {
    assertThrows(IllegalArgumentException.class, () -> Caffeine.newBuilder().initialCapacity(-1));
    Caffeine<Object, Object> builder = Caffeine.newBuilder().initialCapacity(1);
    assertThrows(IllegalStateException.class, () -> builder.initialCapacity(1));

    Caffeine<Object, Object> small = Caffeine.newBuilder().initialCapacity(0);
    assertThat(small.initialCapacity).isEqualTo(0);
    assertThat(small.hasInitialCapacity()).isTrue();
    assertThat(small.getInitialCapacity()).isEqualTo(0);
    assertThat(small.build()).isNotNull();

    Caffeine<Object, Object> def = Caffeine.newBuilder();
    assertThat(def.hasInitialCapacity()).isFalse();
    assertThat(def.getInitialCapacity()).isEqualTo(Caffeine.DEFAULT_INITIAL_CAPACITY);

    Caffeine<Object, Object> large = Caffeine.newBuilder().initialCapacity(Integer.MAX_VALUE);
    assertThat(large.initialCapacity).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void maximumSize() {
    assertThrows(IllegalArgumentException.class, () -> Caffeine.newBuilder().maximumSize(-1));
    Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumSize(1);
    assertThrows(IllegalStateException.class, () -> builder.maximumSize(1));
    assertThrows(
        IllegalStateException.class, () -> Caffeine.newBuilder().maximumWeight(1).maximumSize(1));
    assertThrows(
        IllegalStateException.class,
        () -> Caffeine.newBuilder().weigher(Weigher.singletonWeigher()).maximumSize(1));

    Caffeine<Object, Object> small = Caffeine.newBuilder().maximumSize(0);
    assertThat(small.maximumSize).isEqualTo(0);
    Cache<Object, Object> cache = small.build();
    assertThat(cache.policy().eviction().orElseThrow().getMaximum()).isEqualTo(0);

    Caffeine<Object, Object> large = Caffeine.newBuilder().maximumSize(Integer.MAX_VALUE);
    assertThat(large.build().policy().eviction().orElseThrow().getMaximum())
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void maximumWeightAndWeigher() {
    assertThrows(IllegalArgumentException.class, () -> Caffeine.newBuilder().maximumWeight(-1));
    Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumWeight(1);
    assertThrows(IllegalStateException.class, () -> builder.maximumWeight(1));
    assertThrows(IllegalStateException.class, () -> builder.build());
    assertThrows(
        IllegalStateException.class, () -> Caffeine.newBuilder().maximumSize(1).maximumWeight(1));

    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().weigher(null));
    Caffeine<Object, Object> wBuilder = Caffeine.newBuilder().weigher(Weigher.singletonWeigher());
    assertThrows(IllegalStateException.class, () -> wBuilder.weigher(Weigher.singletonWeigher()));
    assertThrows(IllegalStateException.class, () -> wBuilder.build());
    assertThrows(
        IllegalStateException.class,
        () -> Caffeine.newBuilder().maximumSize(1).weigher(Weigher.singletonWeigher()));

    Caffeine<Object, Object> small =
        Caffeine.newBuilder().maximumWeight(0).weigher(Weigher.singletonWeigher());
    Policy.Eviction<Object, Object> eviction = small.build().policy().eviction().orElseThrow();
    assertThat(eviction.getMaximum()).isEqualTo(0);
    assertThat(eviction.isWeighted()).isTrue();

    Weigher<Object, Object> custom = (k, v) -> 2;
    Caffeine<Object, Object> weighted = Caffeine.newBuilder().maximumWeight(10).weigher(custom);
    assertThat(weighted.isWeighted()).isTrue();
    Weigher<Object, Object> boundedWeigher = weighted.getWeigher();
    assertThat(boundedWeigher).isNotSameInstanceAs(custom);
    assertThat(boundedWeigher.weigh("k", "v")).isEqualTo(2);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Caffeine.newBuilder().maximumWeight(10).weigher((k, v) -> -1).getWeigher().weigh(1, 1));

    assertThat(Caffeine.newBuilder().getWeigher()).isSameInstanceAs(Weigher.singletonWeigher());
    assertThat(Weigher.singletonWeigher().weigh("a", "b")).isEqualTo(1);
    for (SingletonWeigher val : SingletonWeigher.values()) {
      assertThat(SingletonWeigher.valueOf(val.name())).isSameInstanceAs(SingletonWeigher.INSTANCE);
    }
  }

  @Test
  public void expireAfterAccess() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Caffeine.newBuilder().expireAfterAccess(-1, TimeUnit.MILLISECONDS));
    assertThrows(
        NullPointerException.class, () -> Caffeine.newBuilder().expireAfterAccess(1, null));
    Caffeine<Object, Object> builder =
        Caffeine.newBuilder().expireAfterAccess(1, TimeUnit.MILLISECONDS);
    assertThrows(
        IllegalStateException.class, () -> builder.expireAfterAccess(1, TimeUnit.MILLISECONDS));

    assertThat(
            Caffeine.newBuilder()
                .expireAfterAccess(0, TimeUnit.MILLISECONDS)
                .build()
                .policy()
                .expireAfterAccess()
                .orElseThrow()
                .getExpiresAfter(TimeUnit.MILLISECONDS))
        .isEqualTo(0);

    assertThrows(
        IllegalArgumentException.class,
        () -> Caffeine.newBuilder().expireAfterAccess(Duration.ofMillis(-1)));
    assertThrows(
        NullPointerException.class, () -> Caffeine.newBuilder().expireAfterAccess((Duration) null));
    Caffeine<Object, Object> dBuilder =
        Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(1));
    assertThrows(
        IllegalStateException.class, () -> dBuilder.expireAfterAccess(Duration.ofMinutes(1)));
    assertThat(dBuilder.build().policy().expireAfterAccess().orElseThrow().getExpiresAfter())
        .isEqualTo(Duration.ofMinutes(1));

    Caffeine<Object, Object> forever =
        Caffeine.newBuilder().expireAfterAccess(ChronoUnit.FOREVER.getDuration());
    assertThat(forever.getExpiresAfterAccessNanos()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void expireAfterWrite() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Caffeine.newBuilder().expireAfterWrite(-1, TimeUnit.MILLISECONDS));
    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().expireAfterWrite(1, null));
    Caffeine<Object, Object> builder =
        Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MILLISECONDS);
    assertThrows(
        IllegalStateException.class, () -> builder.expireAfterWrite(1, TimeUnit.MILLISECONDS));

    assertThat(
            Caffeine.newBuilder()
                .expireAfterWrite(0, TimeUnit.MILLISECONDS)
                .build()
                .policy()
                .expireAfterWrite()
                .orElseThrow()
                .getExpiresAfter(TimeUnit.MILLISECONDS))
        .isEqualTo(0);

    assertThrows(
        IllegalArgumentException.class,
        () -> Caffeine.newBuilder().expireAfterWrite(Duration.ofMillis(-1)));
    assertThrows(
        NullPointerException.class, () -> Caffeine.newBuilder().expireAfterWrite((Duration) null));
    Caffeine<Object, Object> dBuilder =
        Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(1));
    assertThrows(
        IllegalStateException.class, () -> dBuilder.expireAfterWrite(Duration.ofMinutes(1)));
    assertThat(dBuilder.build().policy().expireAfterWrite().orElseThrow().getExpiresAfter())
        .isEqualTo(Duration.ofMinutes(1));

    Caffeine<Object, Object> forever =
        Caffeine.newBuilder().expireAfterWrite(ChronoUnit.FOREVER.getDuration());
    assertThat(forever.getExpiresAfterWriteNanos()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void schedulerExecutorAndTicker() {
    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().scheduler(null));
    Caffeine<Object, Object> sBuilder =
        Caffeine.newBuilder().scheduler(Scheduler.disabledScheduler());
    assertThrows(
        IllegalStateException.class, () -> sBuilder.scheduler(Scheduler.disabledScheduler()));
    assertThat(Caffeine.newBuilder().getScheduler())
        .isSameInstanceAs(Scheduler.disabledScheduler());

    Caffeine<Object, Object> sysScheduler =
        Caffeine.newBuilder().scheduler(Scheduler.systemScheduler());
    assertThat(sysScheduler.getScheduler()).isSameInstanceAs(Scheduler.systemScheduler());

    Scheduler customScheduler = (exec, task, delay, unit) -> DisabledFuture.instance();
    Caffeine<Object, Object> customSchedBuilder = Caffeine.newBuilder().scheduler(customScheduler);
    assertThat(((GuardedScheduler) customSchedBuilder.getScheduler()).delegate)
        .isSameInstanceAs(customScheduler);

    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().executor(null));
    Caffeine<Object, Object> eBuilder = Caffeine.newBuilder().executor(DIRECT_EXECUTOR);
    assertThrows(IllegalStateException.class, () -> eBuilder.executor(DIRECT_EXECUTOR));
    assertThat(eBuilder.getExecutor()).isSameInstanceAs(DIRECT_EXECUTOR);
    assertThat(Caffeine.newBuilder().getExecutor()).isSameInstanceAs(ForkJoinPool.commonPool());

    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().ticker(null));
    Caffeine<Object, Object> tBuilder = Caffeine.newBuilder().ticker(Ticker.systemTicker());
    assertThrows(IllegalStateException.class, () -> tBuilder.ticker(Ticker.systemTicker()));

    Ticker fakeTicker = new FakeTicker();
    assertThat(Caffeine.newBuilder().ticker(fakeTicker).getTicker())
        .isSameInstanceAs(Ticker.disabledTicker());
    assertThat(
            Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(1))
                .ticker(fakeTicker)
                .getTicker())
        .isSameInstanceAs(fakeTicker);
    assertThat(Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).getTicker())
        .isSameInstanceAs(Ticker.systemTicker());
    assertThat(Caffeine.newBuilder().recordStats().ticker(fakeTicker).getTicker())
        .isSameInstanceAs(Ticker.disabledTicker());

    assertThat(Ticker.systemTicker().read()).isNotEqualTo(0L);
    assertThat(Ticker.disabledTicker().read()).isEqualTo(0L);
    for (SystemTicker val : SystemTicker.values()) {
      assertThat(SystemTicker.valueOf(val.name())).isSameInstanceAs(SystemTicker.INSTANCE);
    }
    for (DisabledTicker val : DisabledTicker.values()) {
      assertThat(DisabledTicker.valueOf(val.name())).isSameInstanceAs(DisabledTicker.INSTANCE);
    }
  }

  @Test
  public void recordStatsAndListeners() {
    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().recordStats(null));
    Supplier<StatsCounter> supplier = () -> Mockito.mock(StatsCounter.class);
    assertThrows(
        IllegalStateException.class, () -> Caffeine.newBuilder().recordStats().recordStats());
    assertThrows(
        IllegalStateException.class,
        () -> Caffeine.newBuilder().recordStats(supplier).recordStats());
    assertThrows(
        IllegalStateException.class,
        () -> Caffeine.newBuilder().recordStats().recordStats(supplier));
    assertThrows(
        IllegalStateException.class,
        () -> Caffeine.newBuilder().recordStats(supplier).recordStats(supplier));

    StatsCounter mockCounter = Mockito.mock(StatsCounter.class);
    Caffeine<Object, Object> customStats = Caffeine.newBuilder().recordStats(() -> mockCounter);
    assertThat(customStats.isRecordingStats()).isTrue();
    StatsCounter guarded = customStats.getStatsCounterSupplier().get();
    guarded.recordEviction(1, RemovalCause.SIZE);
    verify(mockCounter).recordEviction(1, RemovalCause.SIZE);

    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().removalListener(null));
    Caffeine<Object, Object> rBuilder = Caffeine.newBuilder().removalListener((k, v, c) -> {});
    assertThrows(IllegalStateException.class, () -> rBuilder.removalListener((k, v, c) -> {}));

    assertThrows(NullPointerException.class, () -> Caffeine.newBuilder().evictionListener(null));
    RemovalListener<Object, Object> listener = (k, v, c) -> {};
    Caffeine<Object, Object> evBuilder = Caffeine.newBuilder().evictionListener(listener);
    assertThrows(IllegalStateException.class, () -> evBuilder.evictionListener(listener));
    assertThat(evBuilder.getEvictionListener()).isSameInstanceAs(listener);
    assertThat(Caffeine.newBuilder().getEvictionListener()).isNull();
  }

  @Test
  public void ceilingPowerOfTwoAndToNanosSaturated() {
    int[][] intCases = {
      {1, 1},
      {2, 2},
      {3, 4},
      {1024, 1024},
      {1025, 2048},
      {1 << 30, 1 << 30},
      {(1 << 30) + 1, 1 << 30},
      {Integer.MAX_VALUE, 1 << 30},
    };
    for (int[] c : intCases) {
      assertThat(Caffeine.ceilingPowerOfTwo(c[0])).isEqualTo(c[1]);
    }

    long[][] longCases = {
      {1L, 1L},
      {2L, 2L},
      {3L, 4L},
      {1024L, 1024L},
      {1025L, 2048L},
      {1L << 30, 1L << 30},
      {(1L << 30) + 1, 1L << 31},
      {Integer.MAX_VALUE, 1L << 31},
      {1L << 62, 1L << 62},
      {(1L << 62) + 1, 1L << 62},
      {Long.MAX_VALUE, 1L << 62},
    };
    for (long[] c : longCases) {
      assertThat(Caffeine.ceilingPowerOfTwo(c[0])).isEqualTo(c[1]);
    }

    assertThat(Caffeine.toNanosSaturated(Duration.ofNanos(100L))).isEqualTo(100L);
    assertThat(Caffeine.toNanosSaturated(Duration.ofNanos(Long.MAX_VALUE)))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(Caffeine.toNanosSaturated(Duration.ofSeconds(Long.MAX_VALUE / 1_000_000_000, 0)))
        .isLessThan(Long.MAX_VALUE);
    assertThat(Caffeine.toNanosSaturated(Duration.ofDays(365L * 500))).isEqualTo(Long.MAX_VALUE);
    assertThat(Caffeine.toNanosSaturated(Duration.ofNanos(-100L))).isEqualTo(-100L);
    assertThat(Caffeine.toNanosSaturated(Duration.ofNanos(Long.MIN_VALUE)))
        .isEqualTo(Long.MIN_VALUE);
    assertThat(Caffeine.toNanosSaturated(Duration.ofDays(-365L * 500))).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  public void isBoundedAndLoadingVariants() {
    assertThat(Caffeine.newBuilder().isBounded()).isFalse();
    assertThat(Caffeine.newBuilder().maximumSize(10).isBounded()).isTrue();
    assertThat(Caffeine.newBuilder().maximumWeight(10).weigher((k, v) -> 1).isBounded()).isTrue();
    assertThat(Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(1)).isBounded()).isTrue();
    assertThat(Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(1)).isBounded()).isTrue();
    assertThat(Caffeine.newBuilder().evictionListener((k, v, c) -> {}).isBounded()).isFalse();

    assertThat(Caffeine.newBuilder().build(LOADER))
        .isInstanceOf(UnboundedLocalCache.UnboundedLocalLoadingCache.class);
    assertThat(Caffeine.newBuilder().maximumSize(10).build(LOADER))
        .isInstanceOf(BoundedLocalCache.BoundedLocalLoadingCache.class);
  }

  @Test
  public void toUncheckedAndRemovalCause() {
    RuntimeException checkedWrapped = Caffeine.toUnchecked(new IOException("test"));
    assertThat(checkedWrapped).isInstanceOf(CompletionException.class);
    assertThat(checkedWrapped).hasCauseThat().isInstanceOf(IOException.class);
    assertThat(checkedWrapped).hasCauseThat().hasMessageThat().isEqualTo("test");

    IllegalStateException runtime = new IllegalStateException("test");
    assertThat(Caffeine.toUnchecked(runtime)).isSameInstanceAs(runtime);

    assertThrows(AssertionError.class, () -> Caffeine.toUnchecked(new AssertionError("test")));

    InterruptedException interrupted = new InterruptedException("test");
    RuntimeException interruptedWrapped = Caffeine.toUnchecked(interrupted);
    assertThat(interruptedWrapped).isInstanceOf(CompletionException.class);
    assertThat(interruptedWrapped).hasCauseThat().isSameInstanceAs(interrupted);
    assertThat(Thread.interrupted()).isTrue();

    assertThat(RemovalCause.EXPLICIT.wasEvicted()).isFalse();
    assertThat(RemovalCause.REPLACED.wasEvicted()).isFalse();
    assertThat(RemovalCause.COLLECTED.wasEvicted()).isTrue();
    assertThat(RemovalCause.EXPIRED.wasEvicted()).isTrue();
    assertThat(RemovalCause.SIZE.wasEvicted()).isTrue();
    for (RemovalCause cause : RemovalCause.values()) {
      assertThat(RemovalCause.valueOf(cause.name())).isSameInstanceAs(cause);
    }
  }
}
