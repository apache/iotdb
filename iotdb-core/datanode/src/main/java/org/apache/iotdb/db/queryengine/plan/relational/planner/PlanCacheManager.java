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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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

  public static class ProfileDiagnostics {
    private final double ewmaReusablePlanningCost;
    private final double ewmaFirstResponseLatency;
    private final double ewmaBenefitRatio;
    private final long sampleCount;
    private final long hitCount;
    private final long missCount;
    private final long bypassCount;

    private ProfileDiagnostics(
        double ewmaReusablePlanningCost,
        double ewmaFirstResponseLatency,
        double ewmaBenefitRatio,
        long sampleCount,
        long hitCount,
        long missCount,
        long bypassCount) {
      this.ewmaReusablePlanningCost = ewmaReusablePlanningCost;
      this.ewmaFirstResponseLatency = ewmaFirstResponseLatency;
      this.ewmaBenefitRatio = ewmaBenefitRatio;
      this.sampleCount = sampleCount;
      this.hitCount = hitCount;
      this.missCount = missCount;
      this.bypassCount = bypassCount;
    }

    public double getEwmaReusablePlanningCost() {
      return ewmaReusablePlanningCost;
    }

    public double getEwmaFirstResponseLatency() {
      return ewmaFirstResponseLatency;
    }

    public double getEwmaBenefitRatio() {
      return ewmaBenefitRatio;
    }

    public long getSampleCount() {
      return sampleCount;
    }

    public long getHitCount() {
      return hitCount;
    }

    public long getMissCount() {
      return missCount;
    }

    public long getBypassCount() {
      return bypassCount;
    }
  }

  private static class TemplateProfile {
    private static final double EWMA_ALPHA = 0.5;

    private PlanCacheState state = PlanCacheState.MONITOR;
    private boolean warmedUp;
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
      if (cacheLookupMiss) {
        missCount++;
      }

      // Skip the first execution to avoid JVM warmup noise (class loading, JIT,
      // cold caches) that produces unrepresentative planning cost.
      if (!warmedUp) {
        warmedUp = true;
        return state;
      }

      sampleCount++;

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
      } else if (state == PlanCacheState.ACTIVE) {
        // Degrade ACTIVE → MONITOR when benefit drops below admission thresholds.
        // This prevents historical hot templates from polluting the cache indefinitely.
        if (ewmaReusablePlanningCost < minReusablePlanningCost
            || ewmaBenefitRatio < admitRatio) {
          state = PlanCacheState.MONITOR;
          sampleCount = 0; // Reset samples for re-evaluation
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

    synchronized ProfileDiagnostics getDiagnostics() {
      return new ProfileDiagnostics(
          ewmaReusablePlanningCost,
          ewmaFirstResponseLatency,
          ewmaBenefitRatio,
          sampleCount,
          hitCount,
          missCount,
          bypassCount);
    }

    private double ewma(double current, double latest) {
      if (current == 0) {
        return latest;
      }
      return current * (1 - EWMA_ALPHA) + latest * EWMA_ALPHA;
    }
  }

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private int getMaxCacheSize() {
    return CONFIG.getSmartPlanCacheCapacity();
  }

  private long getMaxMemoryBytes() {
    return CONFIG.getSmartPlanCacheMaxMemoryBytes();
  }

  private int getMinSamples() {
    return CONFIG.getSmartPlanCacheMinSamples();
  }

  private long getMinReusablePlanningCostNanos() {
    return CONFIG.getSmartPlanCacheMinReusablePlanningCostNanos();
  }

  private double getAdmitRatio() {
    return CONFIG.getSmartPlanCacheAdmitRatio();
  }

  private double getBypassRatio() {
    return CONFIG.getSmartPlanCacheBypassRatio();
  }

  private long getBypassCooldownNanos() {
    return CONFIG.getSmartPlanCacheBypassCooldownMinutes() * 60L * 1_000_000_000L;
  }

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
    return profile.beforeLookup(System.nanoTime(), getBypassCooldownNanos());
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
        getMinSamples(),
        getMinReusablePlanningCostNanos(),
        getAdmitRatio(),
        getBypassRatio(),
        getBypassCooldownNanos(),
        System.nanoTime());
  }

  public long getEstimatedReusablePlanningCost(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    return profile == null ? 0 : profile.getEstimatedReusablePlanningCost();
  }

  public ProfileDiagnostics getProfileDiagnostics(String cacheKey) {
    TemplateProfile profile = templateProfiles.get(cacheKey);
    return profile == null ? null : profile.getDiagnostics();
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
      while ((currentMemoryBytes.get() > getMaxMemoryBytes()
              || planCache.size() > getMaxCacheSize())
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
