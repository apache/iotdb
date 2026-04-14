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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** LRU cache with lightweight state-machine based admission control. */
public class PlanCacheManager {
  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(PlanCacheManager.class);

  private static class SingletonHolder {
    private static final PlanCacheManager INSTANCE = new PlanCacheManager();
  }

  public enum PlanCacheState {
    MONITOR,
    ACTIVE,
    BYPASS
  }

  public static class LookupDecision {
    private final PlanCacheState state;
    private final boolean shouldLookup;
    private final String reason;

    private LookupDecision(PlanCacheState state, boolean shouldLookup, String reason) {
      this.state = state;
      this.shouldLookup = shouldLookup;
      this.reason = reason;
    }

    public PlanCacheState getState() {
      return state;
    }

    public boolean shouldLookup() {
      return shouldLookup;
    }

    public String getReason() {
      return reason;
    }
  }

  private static class TemplateProfile {
    private static final double EWMA_ALPHA = 0.5;

    private PlanCacheState state = PlanCacheState.MONITOR;
    private long sampleCount;
    private long hitCount;
    private long missCount;
    private long bypassCount;
    private long lastAccessTime;
    private long lastStateChangeTime;
    private long cooldownDeadline;
    private double ewmaReusablePlanningCost;
    private double ewmaFirstResponseLatency;
    private double ewmaBenefitRatio;

    synchronized LookupDecision beforeLookup(long now, long bypassCooldownNanos) {
      if (state == PlanCacheState.BYPASS && now >= cooldownDeadline) {
        state = PlanCacheState.MONITOR;
        lastStateChangeTime = now;
      }
      lastAccessTime = now;
      if (state == PlanCacheState.ACTIVE) {
        return new LookupDecision(state, true, "");
      }
      if (state == PlanCacheState.BYPASS) {
        bypassCount++;
        return new LookupDecision(state, false, "Low_Benefit");
      }
      return new LookupDecision(state, false, "Collecting_Samples");
    }

    synchronized PlanCacheState recordExecution(
        long reusablePlanningCost,
        long firstResponseLatency,
        boolean cacheLookupMiss,
        int minSamples,
        long minReusablePlanningCost,
        double admitRatio,
        double bypassRatio,
        long bypassCooldownNanos,
        long now) {
      lastAccessTime = now;
      sampleCount++;
      if (cacheLookupMiss) {
        missCount++;
      }

      ewmaReusablePlanningCost = ewma(ewmaReusablePlanningCost, reusablePlanningCost);
      ewmaFirstResponseLatency = ewma(ewmaFirstResponseLatency, firstResponseLatency);
      double benefitRatio =
          firstResponseLatency <= 0 ? 0 : ((double) reusablePlanningCost) / firstResponseLatency;
      ewmaBenefitRatio = ewma(ewmaBenefitRatio, benefitRatio);

      if (state == PlanCacheState.MONITOR && sampleCount >= minSamples) {
        if (ewmaReusablePlanningCost >= minReusablePlanningCost && ewmaBenefitRatio >= admitRatio) {
          state = PlanCacheState.ACTIVE;
          lastStateChangeTime = now;
        } else if (ewmaReusablePlanningCost < minReusablePlanningCost
            || ewmaBenefitRatio < bypassRatio) {
          state = PlanCacheState.BYPASS;
          cooldownDeadline = now + bypassCooldownNanos;
          lastStateChangeTime = now;
        }
      }
      return state;
    }

    synchronized void recordCacheHit(long now) {
      hitCount++;
      lastAccessTime = now;
    }

    synchronized PlanCacheState getState() {
      return state;
    }

    synchronized long getEstimatedReusablePlanningCost() {
      return (long) ewmaReusablePlanningCost;
    }

    private double ewma(double current, double latest) {
      if (current == 0) {
        return latest;
      }
      return current * (1 - EWMA_ALPHA) + latest * EWMA_ALPHA;
    }
  }

  private static final int MAX_CACHE_SIZE = 1000;
  private static final long MAX_MEMORY_BYTES = 64L * 1024 * 1024;
  private static final int MIN_SAMPLES = 5;
  private static final long MIN_REUSABLE_PLANNING_COST_NANOS = 1_000_000L;
  private static final double ADMIT_RATIO = 0.20d;
  private static final double BYPASS_RATIO = 0.10d;
  private static final long BYPASS_COOLDOWN_NANOS = 10L * 60L * 1_000_000_000L;

  private final AtomicLong currentMemoryBytes = new AtomicLong(INSTANCE_SIZE);
  private final Map<String, CachedValue> planCache;
  private final Map<String, TemplateProfile> templateProfiles;

  private PlanCacheManager() {
    this.planCache = new LinkedHashMap<>(16, 0.75f, true);
    this.templateProfiles = new ConcurrentHashMap<>();
  }

  public static PlanCacheManager getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public LookupDecision getLookupDecision(String cacheKey) {
    TemplateProfile profile =
        templateProfiles.computeIfAbsent(cacheKey, ignored -> new TemplateProfile());
    return profile.beforeLookup(System.nanoTime(), BYPASS_COOLDOWN_NANOS);
  }

  public void recordCacheHit(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    if (profile != null) {
      profile.recordCacheHit(System.nanoTime());
    }
  }

  public PlanCacheState recordExecution(
      String cacheKey,
      long reusablePlanningCost,
      long firstResponseLatency,
      boolean cacheLookupMiss) {
    TemplateProfile profile =
        templateProfiles.computeIfAbsent(cacheKey, ignored -> new TemplateProfile());
    return profile.recordExecution(
        reusablePlanningCost,
        firstResponseLatency,
        cacheLookupMiss,
        MIN_SAMPLES,
        MIN_REUSABLE_PLANNING_COST_NANOS,
        ADMIT_RATIO,
        BYPASS_RATIO,
        BYPASS_COOLDOWN_NANOS,
        System.nanoTime());
  }

  public long getEstimatedReusablePlanningCost(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    return profile == null ? 0 : profile.getEstimatedReusablePlanningCost();
  }

  public boolean shouldCache(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    return profile != null && profile.getState() == PlanCacheState.ACTIVE;
  }

  public void cacheValue(
      String cachedKey,
      PlanNode planNode,
      List<DeviceTableScanNode> scanNodes,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      int symbolNextId,
      List<List<Expression>> metadataExpressionLists,
      List<List<String>> attributeColumnsLists,
      List<Map<Symbol, ColumnSchema>> assignmentsLists) {
    CachedValue newValue =
        new CachedValue(
            planNode,
            scanNodes,
            literalReference,
            header,
            symbolMap,
            symbolNextId,
            metadataExpressionLists,
            attributeColumnsLists,
            assignmentsLists);

    long keySize = RamUsageEstimator.sizeOf(cachedKey);
    long newValueSize = newValue.estimateMemoryUsage();

    synchronized (planCache) {
      CachedValue oldValue = planCache.put(cachedKey, newValue);
      if (oldValue != null) {
        currentMemoryBytes.addAndGet(-oldValue.estimateMemoryUsage());
      } else {
        currentMemoryBytes.addAndGet(keySize);
      }
      currentMemoryBytes.addAndGet(newValueSize);

      Iterator<Map.Entry<String, CachedValue>> iterator = planCache.entrySet().iterator();
      while ((currentMemoryBytes.get() > MAX_MEMORY_BYTES || planCache.size() > MAX_CACHE_SIZE)
          && iterator.hasNext()) {
        Map.Entry<String, CachedValue> eldest = iterator.next();
        CachedValue evicted = eldest.getValue();
        long evictedKeySize = RamUsageEstimator.sizeOf(eldest.getKey());
        long evictedValueSize = evicted.estimateMemoryUsage();
        iterator.remove();
        currentMemoryBytes.addAndGet(-(evictedKeySize + evictedValueSize));
      }
    }
  }

  public int size() {
    synchronized (planCache) {
      return planCache.size();
    }
  }

  public CachedValue getCachedValue(String cacheKey) {
    synchronized (planCache) {
      return planCache.get(cacheKey);
    }
  }

  public long getCurrentMemoryBytes() {
    return currentMemoryBytes.get();
  }

  @TestOnly
  public PlanCacheState getTemplateState(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    return profile == null ? null : profile.getState();
  }

  public void clear() {
    synchronized (planCache) {
      planCache.clear();
      currentMemoryBytes.set(INSTANCE_SIZE);
    }
    templateProfiles.clear();
  }
}
