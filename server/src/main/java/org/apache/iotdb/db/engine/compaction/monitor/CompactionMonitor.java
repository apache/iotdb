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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionMonitor implements IService {
  private final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private static final CompactionMonitor INSTANCE = new CompactionMonitor();
  // These are constant
  private static final String MONITOR_SG_NAME = "root.compaction_monitor";
  private static final String COMPACTION_CPU_CONSUMPTION_DEVICE =
      MONITOR_SG_NAME.concat(".compaction.cpu");
  private static final String MERGE_CPU_CONSUMPTION_DEVICE = MONITOR_SG_NAME.concat(".merge.cpu");
  private static final String COMPACTION_START_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat(".compaction.start.files");
  private static final String COMPACTION_START_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat(".compaction.start.task_num");
  private static final String MERGE_START_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat(".merge.start.files");
  private static final String MERGE_START_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat(".merge.start.task_num");

  private ScheduledExecutorService threadPool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(1, "CompactionMonitor");
  private long lastUpdateTime = 0L;
  // storage group name -> file level -> compacted file num
  private Map<String, Map<Integer, Integer>> compactionStartFileCountMap = new HashMap<>();
  private Map<String, Integer> compactionStartCountForEachSg = new HashMap<>();
  // it records the total cpu time for all threads
  private long lastCpuTotalTime = 0L;
  // threadId -> cpu time
  private Map<Long, Long> cpuTimeForCompactionThread = new HashMap<>();
  private Set<Long> compactionThreadIdSet = new HashSet<>();
  private Set<Long> mergeThreadIdSet = new HashSet<>();
  private Map<Long, Long> cpuTimeForMergeThread = new HashMap<>();
  private Map<String, Pair<Integer, Integer>> mergeStartFileNumForEachSg = new HashMap<>();
  private Map<String, Integer> mergeStartCountForEachSg = new HashMap<>();
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
          this::sealedMonitorStatusPeriodically,
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          TimeUnit.MILLISECONDS);
      LOGGER.info(
          "[CompactionMonitor] Start to monitor compaction, period is {} ms",
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod());
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

  @Override
  public void waitAndStop(long milliseconds) {
    IService.super.waitAndStop(milliseconds);
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    IService.super.shutdown(milliseconds);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPACTION_MONITOR_SERVICE;
  }

  /** Register compaction thread id to id set */
  public synchronized void registerCompactionThread(long threadId) {
    compactionThreadIdSet.add(threadId);
  }

  public synchronized void registerMergeThread(long threadId) {
    mergeThreadIdSet.add(threadId);
  }

  /**
   * This function should be executed periodically. The interval of calling it is the monitor
   * period. This function will save the monitor data to IoTDB.
   */
  public synchronized void sealedMonitorStatusPeriodically() {
    lastUpdateTime = System.currentTimeMillis();
    Map<Long, Double> cpuConsumptionForCompactionThread =
        calculateCpuConsumptionForCompactionAndMergeThreads();
    saveCpuConsumption(cpuConsumptionForCompactionThread);
    saveCompactionInfo();
    saveMergeInfo();
  }

  public synchronized void reportCompactionStartStatus(
      String storageGroupName, int compactionLevel, int fileNum) {
    if (!compactionThreadIdSet.contains(Thread.currentThread().getId())) {
      registerCompactionThread(Thread.currentThread().getId());
    }
    Map<Integer, Integer> levelFileCountMap =
        compactionStartFileCountMap.computeIfAbsent(storageGroupName, x -> new HashMap<>());
    int newCompactedFileCount = levelFileCountMap.getOrDefault(compactionLevel, 0) + fileNum;
    levelFileCountMap.put(compactionLevel, newCompactedFileCount);
    int newCompactionCount = compactionStartCountForEachSg.getOrDefault(storageGroupName, 0) + 1;
    compactionStartCountForEachSg.put(storageGroupName, newCompactionCount);
  }

  public synchronized void reportMergeStartStatus(
      String storageGroupName, int seqFileNum, int unseqFileNum) {
    mergeStartCountForEachSg.put(
        storageGroupName, mergeStartCountForEachSg.getOrDefault(storageGroupName, 0) + 1);
    Pair<Integer, Integer> mergeFileNumForCurrSg =
        mergeStartFileNumForEachSg.getOrDefault(storageGroupName, new Pair<>(0, 0));
    mergeStartFileNumForEachSg.put(
        storageGroupName,
        new Pair<>(
            mergeFileNumForCurrSg.left + seqFileNum, mergeFileNumForCurrSg.right + unseqFileNum));
  }

  /**
   * Calculate the percentage of cpu consumption during last period for each compaction and merge
   * thread. Meanwhile, call of this function will also update the total cpu time for all threads
   * and total cpu time for each compaction and merge thread.
   *
   * @return A map from threadId to percentage of cpu consumption for each compaction and merge
   *     thread in last monitor period.
   */
  public synchronized Map<Long, Double> calculateCpuConsumptionForCompactionAndMergeThreads() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    Map<Long, Long> cpuTimeForCompactionThreadInThisPeriod = new HashMap<>();
    Map<Long, Long> cpuTimeForMergeThreadInThisPeriod = new HashMap<>();
    long[] allThreadIds = threadMXBean.getAllThreadIds();
    long totalCpuTime = 0L;
    // calculate the cpu time for all threads
    // and store the total cpu time for each thread
    for (long threadId : allThreadIds) {
      long cpuTimeForCurrThread = threadMXBean.getThreadCpuTime(threadId);
      totalCpuTime += cpuTimeForCurrThread;
      if (compactionThreadIdSet.contains(threadId)) {
        cpuTimeForCompactionThreadInThisPeriod.put(threadId, cpuTimeForCurrThread);
      }
      if (mergeThreadIdSet.contains(threadId)) {
        cpuTimeForMergeThreadInThisPeriod.put(threadId, cpuTimeForCurrThread);
      }
    }
    long cpuTimeInThisPeriod = totalCpuTime - lastCpuTotalTime;
    lastCpuTotalTime = totalCpuTime;
    // we use this map to store the cpu consumption percentage of each compaction or merge thread
    Map<Long, Double> cpuConsumptionForCompactionAndMergeThread = new HashMap<>();
    // calculate the cpu consumption of each compaction thread in this period
    // and update the total cpu time for each compaction thread
    for (long threadId : compactionThreadIdSet) {
      cpuConsumptionForCompactionAndMergeThread.put(
          threadId,
          (double)
                  (cpuTimeForCompactionThreadInThisPeriod.get(threadId)
                      - cpuTimeForCompactionThread.getOrDefault(threadId, 0L))
              / (double) (cpuTimeInThisPeriod));
      // update the total cpu time for each compaction thread
      cpuTimeForCompactionThread.put(
          threadId, cpuTimeForCompactionThreadInThisPeriod.get(threadId));
    }

    for (long threadId : mergeThreadIdSet) {
      cpuConsumptionForCompactionAndMergeThread.put(
          threadId,
          (double)
                  (cpuTimeForMergeThreadInThisPeriod.get(threadId)
                      - cpuTimeForMergeThread.getOrDefault(threadId, 0L))
              / (double) (cpuTimeInThisPeriod));
      // update the total cpu time for each merge thread
      cpuTimeForMergeThread.put(threadId, cpuTimeForMergeThreadInThisPeriod.get(threadId));
    }
    return cpuConsumptionForCompactionAndMergeThread;
  }

  private void saveCpuConsumption(Map<Long, Double> consumptionMap) {
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    try {
      // save the cpu consumption of compaction threads
      if (compactionThreadIdSet.size() > 0) {
        PartialPath compactionPath = new PartialPath(COMPACTION_CPU_CONSUMPTION_DEVICE);
        List<String> compactionCpuConsumptionMeasurements = new ArrayList<>();
        List<String> compactionCpuConsumptionValues = new ArrayList<>();
        for (long threadId : compactionThreadIdSet) {
          compactionCpuConsumptionMeasurements.add(mxBean.getThreadInfo(threadId).getThreadName());
          compactionCpuConsumptionValues.add(
              Double.toString(consumptionMap.getOrDefault(threadId, 0.0)));
        }
        InsertRowPlan insertPlanForCompaction =
            new InsertRowPlan(
                compactionPath,
                lastUpdateTime,
                compactionCpuConsumptionMeasurements.toArray(new String[0]),
                compactionCpuConsumptionValues.toArray(new String[0]));
        planExecutor.processNonQuery(insertPlanForCompaction);
      }

      // save the cpu consumption of merge threads
      if (mergeThreadIdSet.size() > 0) {
        PartialPath mergePath = new PartialPath(MERGE_CPU_CONSUMPTION_DEVICE);
        List<String> mergeCpuConsumptionMeasurements = new ArrayList<>();
        List<String> mergeCpuConsumptionValues = new ArrayList<>();
        for (long threadId : mergeThreadIdSet) {
          mergeCpuConsumptionMeasurements.add(mxBean.getThreadInfo(threadId).getThreadName());
          mergeCpuConsumptionValues.add(
              Double.toString(consumptionMap.getOrDefault(threadId, 0.0)));
        }
        InsertRowPlan insertPlanForMerge =
            new InsertRowPlan(
                mergePath,
                lastUpdateTime,
                mergeCpuConsumptionMeasurements.toArray(new String[0]),
                mergeCpuConsumptionValues.toArray(new String[0]));
        planExecutor.processNonQuery(insertPlanForMerge);
      }
    } catch (Throwable e) {
      LOGGER.error("[CompactionMonitor] Exception occurs while saving cpu consumption", e);
    }
  }

  private void saveCompactionInfo() {
    try {
      PartialPath deviceForCompactionCount = new PartialPath(COMPACTION_START_TASK_NUM_DEVICE);
      List<String> measurementsForCompactionCount = new ArrayList<>();
      List<String> valuesForCompactionCount = new ArrayList<>();
      for (String sgName : compactionStartCountForEachSg.keySet()) {
        // remove "root." in sg name
        measurementsForCompactionCount.add(sgName.substring(5).replaceAll("\\.", "#"));
        valuesForCompactionCount.add(Integer.toString(compactionStartCountForEachSg.get(sgName)));
      }
      InsertRowPlan insertRowPlanForCompactionCount =
          new InsertRowPlan(
              deviceForCompactionCount,
              lastUpdateTime,
              measurementsForCompactionCount.toArray(new String[0]),
              valuesForCompactionCount.toArray(new String[0]));
      planExecutor.processNonQuery(insertRowPlanForCompactionCount);
      compactionStartCountForEachSg.clear();

      PartialPath deviceForCompactionFiles = new PartialPath(COMPACTION_START_FILE_NUM_DEVICE);
      List<String> measurementForCompactionFiles = new ArrayList<>();
      List<String> valuesForCompactionFiles = new ArrayList<>();
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      int maxCompactionCount = Math.max(config.getSeqLevelNum(), config.getUnseqLevelNum());
      for (String sgName : compactionStartFileCountMap.keySet()) {
        Map<Integer, Integer> countMap = compactionStartFileCountMap.get(sgName);
        String subPartOfSg = sgName.substring(5).replaceAll("\\.", "#");
        for (int i = 0; i < maxCompactionCount - 1; ++i) {
          int fileCount = countMap.getOrDefault(i, 0);
          measurementForCompactionFiles.add(subPartOfSg + "#level-" + i);
          valuesForCompactionFiles.add(Integer.toString(fileCount));
        }
      }
      InsertRowPlan insertRowPlanForCompactionFileCount =
          new InsertRowPlan(
              deviceForCompactionFiles,
              lastUpdateTime,
              measurementForCompactionFiles.toArray(new String[0]),
              valuesForCompactionFiles.toArray(new String[0]));

      planExecutor.processNonQuery(insertRowPlanForCompactionFileCount);
      compactionStartFileCountMap.clear();

    } catch (Throwable e) {
      LOGGER.error("[CompactionMonitor] Exception occurs while saving compaction info", e);
    }
  }

  private void saveMergeInfo() {
    try {
      PartialPath deviceForMergeCount = new PartialPath(MERGE_START_TASK_NUM_DEVICE);
      List<String> measurementsForMergeCount = new ArrayList<>();
      List<String> valuesForMergeCount = new ArrayList<>();
      for (String sgName : mergeStartCountForEachSg.keySet()) {
        // remove "root." in sg name
        measurementsForMergeCount.add(sgName.substring(5).replaceAll("\\.", "#"));
        valuesForMergeCount.add(Integer.toString(mergeStartCountForEachSg.get(sgName)));
      }
      InsertRowPlan insertRowPlanForMergeCount =
          new InsertRowPlan(
              deviceForMergeCount,
              lastUpdateTime,
              measurementsForMergeCount.toArray(new String[0]),
              valuesForMergeCount.toArray(new String[0]));
      planExecutor.processNonQuery(insertRowPlanForMergeCount);
      mergeStartCountForEachSg.clear();

      PartialPath deviceForMergeFiles = new PartialPath(MERGE_START_FILE_NUM_DEVICE);
      List<String> measurementsForMergeFiles = new ArrayList<>();
      List<String> valuesForMergeFiles = new ArrayList<>();
      for (String sgName : mergeStartFileNumForEachSg.keySet()) {
        Pair<Integer, Integer> fileNumPair = mergeStartFileNumForEachSg.get(sgName);
        // remove "root." in sg name
        measurementsForMergeFiles.add(sgName.substring(5).replaceAll("\\.", "#") + "-seq");
        valuesForMergeFiles.add(Integer.toString(fileNumPair.left));
        measurementsForMergeFiles.add(sgName.substring(5).replaceAll("\\.", "#") + "-unseq");
        valuesForMergeFiles.add(Integer.toString(fileNumPair.right));
      }

      InsertRowPlan insertRowPlanForMergeFiles =
          new InsertRowPlan(
              deviceForMergeFiles,
              lastUpdateTime,
              measurementsForMergeFiles.toArray(new String[0]),
              valuesForMergeFiles.toArray(new String[0]));

      planExecutor.processNonQuery(insertRowPlanForMergeFiles);
      mergeStartFileNumForEachSg.clear();
    } catch (Throwable e) {
      LOGGER.error("[CompactionMonitor] Exception occurs while saving merge info", e);
    }
  }

  public static class CompactionMonitorRegisterTask implements Runnable {
    public boolean isCompactionThread;

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
