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
package org.apache.iotdb.db.engine.compaction.monitor;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionMonitor {
  private final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static final CompactionMonitor INSTANCE = new CompactionMonitor();
  private ScheduledExecutorService threadPool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(1, "CompactionMonitor");
  // storage group name -> file level -> compacted file num
  private Map<String, Map<Integer, Integer>> compactionFileCountMap = new HashMap<>();
  private Map<String, Integer> compactionCountForEachSg = new HashMap<>();
  // it records the total cpu time for all threads
  private long lastCpuTotalTime = 0L;
  // threadId -> cpu time
  private Map<Long, Long> cpuTimeForCompactionThread = new HashMap<>();
  private Set<Long> compactionThreadIdSet = new HashSet<>();
  private Set<Long> mergeThreadIdSet = new HashSet<>();
  private Map<Long, Long> cpuTimeForMergeThread = new HashMap<>();
  private PlanExecutor planExecutor;

  private CompactionMonitor() {
    try {
      this.planExecutor = new PlanExecutor();
    } catch (QueryProcessException e) {
      LOGGER.error("Failed to initialize CompactionMonitor", e);
    }
  }

  public static CompactionMonitor getInstance() {
    return INSTANCE;
  }

  public synchronized void start() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMonitor()) {
      threadPool.scheduleWithFixedDelay(
          this::sealedCompactionStatusPeriodically,
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          TimeUnit.MILLISECONDS);
    }
  }

  public synchronized void stop() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMonitor()) {
      threadPool.shutdownNow();
      try {
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {

      }
      System.out.println("thread pool is stop");
    }
  }

  public synchronized void sealedCompactionStatusPeriodically() {
    if (Thread.interrupted()) {
      Thread.currentThread().interrupt();
    }
    Map<Long, Double> cpuConsumptionForCompactionThread =
        calculateCpuConsumptionForCompactionThreads();
  }

  /** Register compaction thread id to id set */
  public synchronized void registerCompactionThread(long threadId) {
    compactionThreadIdSet.add(threadId);
  }

  public synchronized void registerMergeThread(long threadId) {
    mergeThreadIdSet.add(threadId);
  }

  public synchronized void reportCompactionStatus(
      String storageGroupName, int compactionLevel, int fileNum) {
    if (!compactionThreadIdSet.contains(Thread.currentThread().getId())) {
      registerCompactionThread(Thread.currentThread().getId());
    }
    Map<Integer, Integer> levelFileCountMap =
        compactionFileCountMap.computeIfAbsent(storageGroupName, x -> new HashMap<>());
    int newCompactedFileCount = levelFileCountMap.getOrDefault(compactionLevel, 0) + fileNum;
    levelFileCountMap.put(compactionLevel, newCompactedFileCount);
    int newCompactionCount = compactionCountForEachSg.getOrDefault(storageGroupName, 0) + 1;
    compactionCountForEachSg.put(storageGroupName, newCompactionCount);
  }

  public synchronized Map<Long, Double> calculateCpuConsumptionForCompactionThreads() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    Map<Long, Long> cpuTimeForCompactionThreadInThisPeriod = new HashMap<>();
    long[] allThreadIds = threadMXBean.getAllThreadIds();
    long totalCpuTime = 0L;
    // calculate the cpu time for all threads
    for (long threadId : allThreadIds) {
      long cpuTimeForCurrThread = threadMXBean.getThreadCpuTime(threadId);
      totalCpuTime += cpuTimeForCurrThread;
      if (compactionThreadIdSet.contains(threadId)) {
        cpuTimeForCompactionThreadInThisPeriod.put(threadId, cpuTimeForCurrThread);
      }
    }
    long cpuTimeInThisPeriod = totalCpuTime - lastCpuTotalTime;
    lastCpuTotalTime = totalCpuTime;
    Map<Long, Double> cpuConsumptionForCompactionThread = new HashMap<>();
    // calculate the cpu consumption of each compaction thread in this period
    // and update the total cpu time for each compaction thread
    for (long threadId : compactionThreadIdSet) {
      cpuConsumptionForCompactionThread.put(
          threadId,
          (double)
                  (cpuTimeForCompactionThreadInThisPeriod.get(threadId)
                      - cpuTimeForCompactionThread.getOrDefault(threadId, 0L))
              / (double) (cpuTimeInThisPeriod));
      cpuTimeForCompactionThread.put(
          threadId, cpuTimeForCompactionThreadInThisPeriod.get(threadId));
    }
    return cpuConsumptionForCompactionThread;
  }

  public static class CompactionMonitorRegisterTask implements Runnable {
    public boolean isCompactionThread = true;

    public CompactionMonitorRegisterTask(boolean isCompactionThread) {
      this.isCompactionThread = isCompactionThread;
    }

    @Override
    public void run() {
      CompactionMonitor monitor = CompactionMonitor.getInstance();
      try {
        // Sleep for 10 seconds to avoid registering twice in the same thread
        Thread.sleep(10_000);
      } catch (Exception e) {
      }
      if (isCompactionThread) {
        monitor.registerCompactionThread(Thread.currentThread().getId());
      } else {
        monitor.registerMergeThread(Thread.currentThread().getId());
      }
    }
  }
}
