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

package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.commons.binaryallocator.BinaryAllocator;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class JvmGcMonitorMetrics implements IMetricSet {
  // Duration of observation window
  public static final long OBSERVATION_WINDOW_MS = TimeUnit.SECONDS.toMillis(30);
  // Interval for data collection
  public static final long SLEEP_INTERVAL_MS = TimeUnit.SECONDS.toMillis(3);
  // Max GC time threshold
  public static final long MAX_GC_TIME_PERCENTAGE = 30L;
  // The time when JvmGcMonitorMetrics start running
  private static long monitorStartTime;
  private static final Logger logger = LoggerFactory.getLogger(JvmGcMonitorMetrics.class);
  private final ScheduledExecutorService scheduledGCInfoMonitor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.JVM_GC_STATISTICS_MONITOR.getName());
  private Future<?> scheduledGcMonitorFuture;
  // Ring buffers containing GC timings and timestamps when timings were taken
  private final TsAndData[] gcDataBuf;
  // Buffer size
  private final int bufSize;
  // Buffer start index
  private int startIdx;
  // Buffer end index
  private int endIdx;
  // Container to hold collected GC data
  private final GcData curData = new GcData();
  // Hook function called with GC exception
  private final GcTimeAlertHandler alertHandler;

  private JvmGcMonitorMetrics() {
    bufSize = (int) (OBSERVATION_WINDOW_MS / SLEEP_INTERVAL_MS + 2);
    // Prevent the user from accidentally creating an abnormally big buffer, which will result in
    // slow calculations and likely inaccuracy.
    Preconditions.checkArgument(bufSize <= 128 * 1024);
    gcDataBuf = new TsAndData[bufSize];
    for (int i = 0; i < bufSize; i++) {
      gcDataBuf[i] = new TsAndData();
    }

    alertHandler = new GcTimeAlerter();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        SystemMetric.JVM_GC_ACCUMULATED_TIME_PERCENTAGE.toString(),
        MetricLevel.CORE,
        curData,
        GcData::getGcTimePercentage);

    monitorStartTime = System.currentTimeMillis();
    // Set start time's accumulated GC Time
    curData.setAccumulatedGcTime(getTotalGCTime());
    // current collect time: startTime + start delay(50ms)
    gcDataBuf[startIdx].setValues(monitorStartTime + TimeUnit.MILLISECONDS.toMillis(50), 0);
    scheduledGcMonitorFuture =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            scheduledGCInfoMonitor,
            this::scheduledMonitoring,
            TimeUnit.MILLISECONDS.toMillis(50), // to prevent / ZERO exception
            SLEEP_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, SystemMetric.JVM_GC_ACCUMULATED_TIME_PERCENTAGE.toString());
    if (scheduledGcMonitorFuture != null) {
      scheduledGcMonitorFuture.cancel(false);
      scheduledGcMonitorFuture = null;
      logger.info("JVM GC scheduled monitor is stopped successfully.");
    }
  }

  private void scheduledMonitoring() {
    calculateGCTimePercentageWithinObservedInterval();

    // Alert if necessary
    if (alertHandler != null && curData.getGcTimePercentage() > MAX_GC_TIME_PERCENTAGE) {
      alertHandler.alert(curData.clone());
    }

    // Run GC eviction
    BinaryAllocator.getInstance().runGcEviction(curData.getGcTimePercentage());
  }

  private long getTotalGCTime() {
    long totalGcTime = 0;
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      totalGcTime += gcBean.getCollectionTime();
    }
    return totalGcTime;
  }

  private void calculateGCTimePercentageWithinObservedInterval() {
    long prevTotalGcTime = curData.getAccumulatedGcTime();
    long totalGcTime = getTotalGCTime();

    long gcTimeWithinSleepInterval = totalGcTime - prevTotalGcTime;
    long curTime = System.currentTimeMillis();
    long gcMonitorRunTime = curTime - monitorStartTime;

    endIdx = (endIdx + 1) % bufSize;
    gcDataBuf[endIdx].setValues(curTime, gcTimeWithinSleepInterval);

    // Move startIdx forward until we reach the first buffer entry with
    // timestamp within the observation window.
    long startObsWindowTs = curTime - OBSERVATION_WINDOW_MS;
    while (gcDataBuf[startIdx].ts < startObsWindowTs && startIdx != endIdx) {
      startIdx = (startIdx + 1) % bufSize;
    }

    // Calculate total GC time within observationWindowMs.
    // We should be careful about GC time that passed before the first timestamp
    // in our observation window.
    long gcTimeWithinObservationWindow =
        Math.min(gcDataBuf[startIdx].gcPause, gcDataBuf[startIdx].ts - startObsWindowTs);
    if (startIdx != endIdx) {
      for (int i = (startIdx + 1) % bufSize; i != endIdx; i = (i + 1) % bufSize) {
        gcTimeWithinObservationWindow += gcDataBuf[i].gcPause;
      }
    }

    curData.update(
        curTime,
        startObsWindowTs,
        totalGcTime,
        gcTimeWithinObservationWindow,
        (int)
            (gcTimeWithinObservationWindow
                * 100
                / Math.min(OBSERVATION_WINDOW_MS, gcMonitorRunTime)));
  }

  public GcData getGcData() {
    return curData;
  }

  /** Encapsulates data about GC pauses measured at the specific timestamp. */
  public static class GcData implements Cloneable {
    // The time when this object get updated.
    private final AtomicLong timestamp = new AtomicLong();
    // The theoretical start time of the observation window, usually equal to `timestamp -
    // OBSERVATION_WINDOW_MS`
    private final AtomicLong startObsWindowTs = new AtomicLong();
    // Accumulated GC time since the start of IoTDB.
    private final AtomicLong accumulatedGcTime = new AtomicLong();
    // The percentage (0..100) of time that the JVM spent in GC pauses within the observation window
    private final AtomicLong gcTimePercentage = new AtomicLong();
    // Accumulated GC time within the latest observation window.
    private final AtomicLong gcTimeWithinObsWindow = new AtomicLong();

    /**
     * Returns the length of current observation window, usually equal to OBSERVATION_WINDOW_MS. If
     * IoTDB is started after the start of the theoretical time window, then IoTDB startup time is
     * returned.
     *
     * @return current observation window time, millisecond.
     */
    public long getCurrentObsWindowTs() {
      return Math.min(timestamp.get() - monitorStartTime, timestamp.get() - startObsWindowTs.get());
    }

    /**
     * Returns the absolute timestamp when this measurement was taken.
     *
     * @return timestamp.
     */
    public long getTimestamp() {
      return timestamp.get();
    }

    /**
     * Returns the start timestamp of the latest observation window.
     *
     * @return the actual start timestamp of the obs window.
     */
    public long getStartObsWindowTs() {
      return Math.max(startObsWindowTs.get(), monitorStartTime);
    }

    /**
     * Returns accumulated GC time since the start of IoTDB.
     *
     * @return AccumulatedGcTime.
     */
    public long getAccumulatedGcTime() {
      return accumulatedGcTime.get();
    }

    /**
     * Returns accumulated GC time within the latest observation window.
     *
     * @return gcTimeWithinObsWindow.
     */
    public long getGcTimeWithinObsWindow() {
      return gcTimeWithinObsWindow.get();
    }

    /**
     * Returns the percentage (0..100) of time that the JVM spent in GC pauses within the
     * observation window of the associated GCTimeMonitor.
     *
     * @return GcTimePercentage.
     */
    public long getGcTimePercentage() {
      return gcTimePercentage.get();
    }

    private void setAccumulatedGcTime(long accumulatedGcTime) {
      this.accumulatedGcTime.set(accumulatedGcTime);
    }

    private synchronized void update(
        long inTimestamp,
        long inStartObsWindowTs,
        long inTotalGcTime,
        long inGcTimeWithinObsWindow,
        int inGcTimePercentage) {
      this.timestamp.set(inTimestamp);
      this.startObsWindowTs.set(inStartObsWindowTs);
      this.accumulatedGcTime.set(inTotalGcTime);
      this.gcTimeWithinObsWindow.set(inGcTimeWithinObsWindow);
      this.gcTimePercentage.set(inGcTimePercentage);
    }

    @Override
    public synchronized GcData clone() {
      try {
        return (GcData) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class TsAndData {
    // Timestamp when this measurement was taken
    private long ts;
    // Total GC pause time within the interval between ts
    // and the timestamp of the previous measurement.
    private long gcPause;

    void setValues(long inTs, long inGcPause) {
      this.ts = inTs;
      this.gcPause = inGcPause;
    }
  }

  /**
   * The user can provide an instance of a class implementing this interface when initializing a
   * GCTimeMonitor to receive alerts when GC time percentage exceeds the specified threshold.
   */
  public interface GcTimeAlertHandler {
    void alert(GcData gcData);
  }

  private static class JvmGcMetricsHolder {

    private static final JvmGcMonitorMetrics INSTANCE = new JvmGcMonitorMetrics();

    private JvmGcMetricsHolder() {
      // empty constructor
    }
  }

  public static JvmGcMonitorMetrics getInstance() {
    return JvmGcMetricsHolder.INSTANCE;
  }
}
