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

package org.apache.iotdb.metrics.metricsets.jvm;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** This file is modified from io.micrometer.core.instrument.binder.jvm.JvmGcMetrics */
public class JvmGcMetrics implements IMetricSet, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(JvmGcMetrics.class);
  private String youngGenPoolName;
  private String oldGenPoolName;
  private String nonGenerationalMemoryPool;
  private final List<Runnable> notificationListenerCleanUpRunnables = new CopyOnWriteArrayList<>();

  public JvmGcMetrics() {
    for (MemoryPoolMXBean mbean : ManagementFactory.getMemoryPoolMXBeans()) {
      String name = mbean.getName();
      if (isYoungGenPool(name)) {
        youngGenPoolName = name;
      } else if (isOldGenPool(name)) {
        oldGenPoolName = name;
      } else if (isNonGenerationalHeapPool(name)) {
        nonGenerationalMemoryPool = name;
      }
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    if (!preCheck()) {
      return;
    }

    double maxLongLivedPoolBytes =
        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class).stream()
            .filter(mem -> MemoryType.HEAP.equals(mem.getType()))
            .filter(mem -> isOldGenPool(mem.getName()) || isNonGenerationalHeapPool(mem.getName()))
            .findAny()
            .map(mem -> JvmUtils.getUsageValue(mem, MemoryUsage::getMax))
            .orElse(0.0);

    AtomicLong maxDataSize = new AtomicLong((long) maxLongLivedPoolBytes);
    metricService.createAutoGauge(
        "jvm_gc_max_data_size_bytes", MetricLevel.CORE, maxDataSize, AtomicLong::get);

    AtomicLong liveDataSize = new AtomicLong();
    metricService.createAutoGauge(
        "jvm_gc_live_data_size_bytes", MetricLevel.CORE, liveDataSize, AtomicLong::get);

    Counter allocatedBytes =
        metricService.getOrCreateCounter("jvm_gc_memory_allocated_bytes", MetricLevel.CORE);

    Counter promotedBytes =
        (oldGenPoolName == null)
            ? null
            : metricService.getOrCreateCounter("jvm_gc_memory_promoted_bytes", MetricLevel.CORE);

    // start watching for GC notifications
    final AtomicLong heapPoolSizeAfterGc = new AtomicLong();

    for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (!(mbean instanceof NotificationEmitter)) {
        continue;
      }
      NotificationListener notificationListener =
          (notification, ref) -> {
            CompositeData cd = (CompositeData) notification.getUserData();
            GarbageCollectionNotificationInfo notificationInfo =
                GarbageCollectionNotificationInfo.from(cd);

            String gcCause = notificationInfo.getGcCause();
            String gcAction = notificationInfo.getGcAction();
            GcInfo gcInfo = notificationInfo.getGcInfo();
            long duration = gcInfo.getDuration();
            String timerName;
            if (isConcurrentPhase(gcCause, notificationInfo.getGcName())) {
              timerName = "jvm_gc_concurrent_phase_time";
            } else {
              timerName = "jvm_gc_pause";
            }
            Timer timer =
                metricService.getOrCreateTimer(
                    timerName, MetricLevel.CORE, "action", gcAction, "cause", gcCause);
            timer.update(duration, TimeUnit.MILLISECONDS);

            // Update promotion and allocation counters
            final Map<String, MemoryUsage> before = gcInfo.getMemoryUsageBeforeGc();
            final Map<String, MemoryUsage> after = gcInfo.getMemoryUsageAfterGc();

            if (nonGenerationalMemoryPool != null) {
              countPoolSizeDelta(
                  gcInfo.getMemoryUsageBeforeGc(),
                  gcInfo.getMemoryUsageAfterGc(),
                  allocatedBytes,
                  heapPoolSizeAfterGc,
                  nonGenerationalMemoryPool);
              if (after.get(nonGenerationalMemoryPool).getUsed()
                  < before.get(nonGenerationalMemoryPool).getUsed()) {
                liveDataSize.set(after.get(nonGenerationalMemoryPool).getUsed());
                final long longLivedMaxAfter = after.get(nonGenerationalMemoryPool).getMax();
                maxDataSize.set(longLivedMaxAfter);
              }
              return;
            }

            if (oldGenPoolName != null) {
              final long oldBefore = before.get(oldGenPoolName).getUsed();
              final long oldAfter = after.get(oldGenPoolName).getUsed();
              final long delta = oldAfter - oldBefore;
              if (delta > 0L && promotedBytes != null) {
                promotedBytes.inc(delta);
              }

              // Some GC implementations such as G1 can reduce the old gen size as part of a minor
              // GC. To track the
              // live data size we record the value if we see a reduction in the old gen heap size
              // or
              // after a major GC.
              if (oldAfter < oldBefore
                  || GcGenerationAge.fromName(notificationInfo.getGcName())
                      == GcGenerationAge.OLD) {
                liveDataSize.set(oldAfter);
                final long oldMaxAfter = after.get(oldGenPoolName).getMax();
                maxDataSize.set(oldMaxAfter);
              }
            }

            if (youngGenPoolName != null) {
              countPoolSizeDelta(
                  gcInfo.getMemoryUsageBeforeGc(),
                  gcInfo.getMemoryUsageAfterGc(),
                  allocatedBytes,
                  heapPoolSizeAfterGc,
                  youngGenPoolName);
            }
          };
      NotificationEmitter notificationEmitter = (NotificationEmitter) mbean;
      notificationEmitter.addNotificationListener(
          notificationListener,
          notification ->
              notification
                  .getType()
                  .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION),
          null);
      notificationListenerCleanUpRunnables.add(
          () -> {
            try {
              notificationEmitter.removeNotificationListener(notificationListener);
            } catch (ListenerNotFoundException ignore) {
              // do nothing
            }
          });
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    if (!preCheck()) {
      return;
    }

    metricService.remove(MetricType.AUTO_GAUGE, "jvm_gc_max_data_size_bytes");
    metricService.remove(MetricType.AUTO_GAUGE, "jvm_gc_live_data_size_bytes");
    metricService.remove(MetricType.COUNTER, "jvm_gc_memory_allocated_bytes");

    if (oldGenPoolName != null) {
      metricService.remove(MetricType.COUNTER, "jvm_gc_memory_promoted_bytes");
    }

    // start watching for GC notifications
    for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (!(mbean instanceof NotificationEmitter)) {
        continue;
      }
      NotificationListener notificationListener =
          (notification, ref) -> {
            CompositeData cd = (CompositeData) notification.getUserData();
            GarbageCollectionNotificationInfo notificationInfo =
                GarbageCollectionNotificationInfo.from(cd);

            String gcCause = notificationInfo.getGcCause();
            String gcAction = notificationInfo.getGcAction();
            String timerName;
            if (isConcurrentPhase(gcCause, notificationInfo.getGcName())) {
              timerName = "jvm_gc_concurrent_phase_time";
            } else {
              timerName = "jvm_gc_pause";
            }
            metricService.remove(MetricType.TIMER, timerName, "action", gcAction, "cause", gcCause);
          };
      NotificationEmitter notificationEmitter = (NotificationEmitter) mbean;
      notificationEmitter.addNotificationListener(
          notificationListener,
          notification ->
              notification
                  .getType()
                  .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION),
          null);
      notificationListenerCleanUpRunnables.add(
          () -> {
            try {
              notificationEmitter.removeNotificationListener(notificationListener);
            } catch (ListenerNotFoundException ignore) {
              // do nothing
            }
          });
    }
  }

  private boolean preCheck() {
    if (ManagementFactory.getMemoryPoolMXBeans().isEmpty()) {
      logger.warn(
          "GC notifications will not be available because MemoryPoolMXBeans are not provided by the JVM");
      return false;
    }

    try {
      Class.forName(
          "com.sun.management.GarbageCollectionNotificationInfo",
          false,
          MemoryPoolMXBean.class.getClassLoader());
    } catch (Throwable e) {
      // We are operating in a JVM without access to this level of detail
      logger.warn(
          "GC notifications will not be available because "
              + "com.sun.management.GarbageCollectionNotificationInfo is not present");
      return false;
    }
    return true;
  }

  private void countPoolSizeDelta(
      Map<String, MemoryUsage> before,
      Map<String, MemoryUsage> after,
      Counter counter,
      AtomicLong previousPoolSize,
      String poolName) {
    final long beforeBytes = before.get(poolName).getUsed();
    final long afterBytes = after.get(poolName).getUsed();
    final long delta = beforeBytes - previousPoolSize.get();
    previousPoolSize.set(afterBytes);
    if (delta > 0L) {
      counter.inc(delta);
    }
  }

  @Override
  public void close() {
    notificationListenerCleanUpRunnables.forEach(Runnable::run);
  }

  enum GcGenerationAge {
    OLD,
    YOUNG,
    UNKNOWN;

    private static Map<String, GcGenerationAge> knownCollectors =
        new HashMap<String, GcGenerationAge>() {
          {
            put("ConcurrentMarkSweep", OLD);
            put("Copy", YOUNG);
            put("G1 Old Generation", OLD);
            put("G1 Young Generation", YOUNG);
            put("MarkSweepCompact", OLD);
            put("PS MarkSweep", OLD);
            put("PS Scavenge", YOUNG);
            put("ParNew", YOUNG);
          }
        };

    static GcGenerationAge fromName(String name) {
      return knownCollectors.getOrDefault(name, UNKNOWN);
    }
  }

  private static Optional<MemoryPoolMXBean> getLongLivedHeapPool() {
    return ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean.class).stream()
        .filter(JvmGcMetrics::isHeap)
        .filter(mem -> isOldGenPool(mem.getName()) || isNonGenerationalHeapPool(mem.getName()))
        .findAny();
  }

  private static boolean isConcurrentPhase(String cause, String name) {
    return "No GC".equals(cause) || "Shenandoah Cycles".equals(name);
  }

  private static boolean isYoungGenPool(String name) {
    return name != null && name.endsWith("Eden Space");
  }

  private static boolean isOldGenPool(String name) {
    return name != null && (name.endsWith("Old Gen") || name.endsWith("Tenured Gen"));
  }

  private static boolean isNonGenerationalHeapPool(String name) {
    return "Shenandoah".equals(name) || "ZHeap".equals(name);
  }

  private static boolean isHeap(MemoryPoolMXBean memoryPoolBean) {
    return MemoryType.HEAP.equals(memoryPoolBean.getType());
  }
}
