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
import org.apache.iotdb.metrics.utils.SystemMetric;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** This file is modified from io.micrometer.core.instrument.binder.jvm.JvmGcMetrics */
public class JvmGcMetrics implements IMetricSet, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(JvmGcMetrics.class);
  private final List<Runnable> notificationListenerCleanUpRunnables = new CopyOnWriteArrayList<>();
  private String youngGenPoolName;
  private String oldGenPoolName;
  private String nonGenerationalMemoryPool;
  private final AtomicLong lastGcTotalDuration = new AtomicLong();
  private final AtomicLong totalGcTimeSpend = new AtomicLong();
  private double throughout = 0.0d;

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

  private static boolean isPartiallyConcurrentGC(GarbageCollectorMXBean gc) {
    switch (gc.getName()) {
        // First two are from the 'serial' collector which are not concurrent, obviously.
      case "Copy":
      case "MarkSweepCompact":
        // The following 4 GCs do not contain concurrent execution phase.
      case "PS MarkSweep":
      case "PS Scavenge":
      case "G1 Young Generation":
      case "ParNew":
        return false;

        // The following 2 GCs' execution process consists of concurrent phase, which means they can
        // run simultaneously with the user thread in some phases.

        // Concurrent mark and concurrent sweep
      case "ConcurrentMarkSweep":
        // Concurrent mark
      case "G1 Old Generation":
        return true;
      default:
        // Assume possibly concurrent if unsure
        return true;
    }
  }

  private static boolean isConcurrentPhase(String cause, String name) {
    return "No GC".equals(cause) || "Shenandoah Cycles".equals(name);
  }

  private static boolean isYoungGenPool(String name) {
    return name != null && (name.endsWith("Eden Space") || name.endsWith("Survivor Space"));
  }

  private static boolean isOldGenPool(String name) {
    return name != null && (name.endsWith("Old Gen") || name.endsWith("Tenured Gen"));
  }

  private static boolean isNonGenerationalHeapPool(String name) {
    return "Shenandoah".equals(name) || "ZHeap".equals(name);
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
        SystemMetric.JVM_GC_MAX_DATA_SIZE_BYTES.toString(),
        MetricLevel.CORE,
        maxDataSize,
        AtomicLong::get);

    AtomicLong liveDataSize = new AtomicLong();
    metricService.createAutoGauge(
        SystemMetric.JVM_GC_LIVE_DATA_SIZE_BYTES.toString(),
        MetricLevel.CORE,
        liveDataSize,
        AtomicLong::get);

    AtomicLong heapMemUsedPercentage = new AtomicLong(calculateMemoryUsagePercentage());
    metricService.createAutoGauge(
        SystemMetric.JVM_GC_MEMORY_USED_PERCENT.toString(),
        MetricLevel.CORE,
        heapMemUsedPercentage,
        AtomicLong::get);

    metricService.createAutoGauge(
        SystemMetric.JVM_GC_THROUGHOUT.toString(),
        MetricLevel.CORE,
        this,
        JvmGcMetrics::getThroughput);

    Counter allocatedBytes =
        metricService.getOrCreateCounter(
            SystemMetric.JVM_GC_MEMORY_ALLOCATED_BYTES.toString(), MetricLevel.CORE);

    Counter promotedBytes =
        (oldGenPoolName == null)
            ? null
            : metricService.getOrCreateCounter(
                SystemMetric.JVM_GC_MEMORY_PROMOTED_BYTES.toString(), MetricLevel.CORE);

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

            // The duration supplied in the notification info includes more than just
            // application stopped time for concurrent GCs (since the concurrent phase is not
            // stop-the-world).
            // E.g. For mixed GC or full GC in collector 'G1 old generation', the duration collected
            // here is more than the actual pause time (the latter can be accessed by GC
            // log/-XX:PrintGCDetails)
            long duration = gcInfo.getDuration();

            // Try and do a better job coming up with a good stopped time
            // value by asking for and tracking cumulative time spent blocked in GC.
            if (isPartiallyConcurrentGC(mbean)) {
              long previousTotal = lastGcTotalDuration.get();
              long total = mbean.getCollectionTime();
              lastGcTotalDuration.set(total);
              duration = total - previousTotal; // may be zero for a really fast collection
            }
            totalGcTimeSpend.set(totalGcTimeSpend.get() + duration);

            String timerName;
            if (isConcurrentPhase(gcCause, notificationInfo.getGcName())) {
              timerName = "jvm_gc_concurrent_phase_time";
            } else {
              timerName = "jvm_gc_pause";
            }
            // create a timer with tags named by gcCause, which binds gcCause with gcDuration
            Timer timer =
                metricService.getOrCreateTimer(
                    timerName, MetricLevel.CORE, "action", gcAction, "cause", gcCause);
            timer.update(duration, TimeUnit.MILLISECONDS);

            // add support for ZGC
            if (mbean.getName().equals("ZGC Cycles")) {
              Counter cyclesCount =
                  metricService.getOrCreateCounter(
                      SystemMetric.JVM_ZGC_CYCLES_COUNT.toString(), MetricLevel.CORE);
              cyclesCount.inc();
            } else if (mbean.getName().equals("ZGC Pauses")) {
              Counter pausesCount =
                  metricService.getOrCreateCounter(
                      SystemMetric.JVM_ZGC_PAUSES_COUNT.toString(), MetricLevel.CORE);
              pausesCount.inc();
            }

            // monitoring old/young GC count, which is helpful for users to locate GC exception.
            // Unfortunately, the JMX doesn't seem to provide an api for monitoring mixed gc in G1.
            // In fact, JMX may treat mixed GCs as minor GCs.
            if (GcGenerationAge.fromName(notificationInfo.getGcName()) == GcGenerationAge.OLD) {
              Counter oldGcCounter =
                  metricService.getOrCreateCounter(
                      SystemMetric.JVM_GC_YOUNG_GC_COUNT.toString(), MetricLevel.CORE);
              oldGcCounter.inc();
            } else if (GcGenerationAge.fromName(notificationInfo.getGcName())
                == GcGenerationAge.YOUNG) {
              Counter youngGcCounter =
                  metricService.getOrCreateCounter(
                      SystemMetric.JVM_GC_OLD_GC_COUNT.toString(), MetricLevel.CORE);
              youngGcCounter.inc();
            }

            // update memory usage percentage
            heapMemUsedPercentage.set(calculateMemoryUsagePercentage());

            // update throughput, which is the
            // percentage of GC time as total JVM running time
            throughout =
                (double) ((gcInfo.getEndTime() - totalGcTimeSpend.get()))
                    / gcInfo.getEndTime()
                    * 100L;

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
            }

            // should add `else` here, since there are only two
            // cases: generational and non-generational
            else {
              if (oldGenPoolName != null) {
                final long oldBefore = before.get(oldGenPoolName).getUsed();
                final long oldAfter = after.get(oldGenPoolName).getUsed();
                final long delta = oldAfter - oldBefore;
                if (delta > 0L && promotedBytes != null) {
                  promotedBytes.inc(delta);
                }

                // Some GC implementations such as G1 can reduce the old gen size as part of a minor
                // GC (since in JMX, a minor GC of G1 may actually represent mixed GC, which collect
                // some obj in old gen region). To track the
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

    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_GC_MAX_DATA_SIZE_BYTES.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, SystemMetric.JVM_GC_LIVE_DATA_SIZE_BYTES.toString());
    metricService.remove(MetricType.COUNTER, SystemMetric.JVM_GC_MEMORY_ALLOCATED_BYTES.toString());
    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_GC_MEMORY_USED_PERCENT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, SystemMetric.JVM_GC_THROUGHOUT.toString());

    if (oldGenPoolName != null) {
      metricService.remove(
          MetricType.COUNTER, SystemMetric.JVM_GC_MEMORY_PROMOTED_BYTES.toString());
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
            if (mbean.getName().equals("ZGC Cycles")) {
              metricService.remove(
                  MetricType.COUNTER, SystemMetric.JVM_ZGC_CYCLES_COUNT.toString());
            } else if (mbean.getName().equals("ZGC Pauses")) {
              metricService.remove(
                  MetricType.COUNTER, SystemMetric.JVM_ZGC_PAUSES_COUNT.toString());
            }
            if (GcGenerationAge.fromName(notificationInfo.getGcName()) == GcGenerationAge.OLD) {
              metricService.remove(
                  MetricType.COUNTER, SystemMetric.JVM_GC_YOUNG_GC_COUNT.toString());
            } else if (GcGenerationAge.fromName(notificationInfo.getGcName())
                == GcGenerationAge.YOUNG) {
              metricService.remove(MetricType.COUNTER, SystemMetric.JVM_GC_OLD_GC_COUNT.toString());
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

  private boolean preCheck() {
    if (ManagementFactory.getMemoryPoolMXBeans().isEmpty()) {
      logger.warn(
          "GC notifications will not be available because MemoryPoolMXBeans "
              + "are not provided by the JVM");
      return false;
    }

    try {
      Class.forName(
          "com.sun.management.GarbageCollectionNotificationInfo",
          false,
          MemoryPoolMXBean.class.getClassLoader());
    } catch (Exception e) {
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

  public long calculateMemoryUsagePercentage() {
    return (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()
        * 100
        / Runtime.getRuntime().maxMemory());
  }

  // We need to keep two decimal places accurate, so we maintain a double var here.
  public double getThroughput() {
    return throughout;
  }

  enum GcGenerationAge {
    OLD,
    YOUNG,
    UNKNOWN;

    private static final Map<String, GcGenerationAge> knownCollectors = new HashMap<>();

    static {
      knownCollectors.put("ConcurrentMarkSweep", OLD);
      knownCollectors.put("Copy", YOUNG);
      knownCollectors.put("G1 Old Generation", OLD);
      knownCollectors.put("G1 Young Generation", YOUNG);
      knownCollectors.put("MarkSweepCompact", OLD);
      knownCollectors.put("PS MarkSweep", OLD);
      knownCollectors.put("PS Scavenge", YOUNG);
      knownCollectors.put("ParNew", YOUNG);
    }

    static GcGenerationAge fromName(String name) {
      return knownCollectors.getOrDefault(name, UNKNOWN);
    }
  }
}
