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
  private final Logger LOGGER = LoggerFactory.getLogger(CompactionMonitor.class);
  private static final CompactionMonitor INSTANCE = new CompactionMonitor();
  // These are constant
  private static final String MONITOR_SG_NAME = "root.compaction_monitor";
  private static final String COMPACTION_CPU_CONSUMPTION_DEVICE =
      MONITOR_SG_NAME.concat(".compaction.cpu");
  private static final String MERGE_CPU_CONSUMPTION_DEVICE = MONITOR_SG_NAME.concat(".merge.cpu");
  private static final String COMPACTION_BEGIN_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.compaction.begin.files");
  private static final String COMPACTION_BEGIN_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.compaction.begin.task_num");
  private static final String COMPACTION_FINISH_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.compaction.finish.files");
  private static final String COMPACTION_FINISH_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.compaction.finish.task_num");
  private static final String MERGE_BEGIN_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.merge.begin.files");
  private static final String MERGE_BEGIN_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.merge.begin.task_num");
  private static final String MERGE_FINISH_FILE_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.merge.finish.files");
  private static final String MERGE_FINISH_TASK_NUM_DEVICE =
      MONITOR_SG_NAME.concat("%s.merge.finish.task_num");
  private static final String COMPACTION_CPU_CONSUMPTION_SUM_MEASUREMENT = "Compaction-Total";
  private static final String MERGE_CPU_CONSUMPTION_SUM_MEASUREMENT = "Merge-Total";
  private static final long COMPACTION_CPU_CONSUMPTION_TOTAL_MAP_KEY = -1;
  private static final long MERGE_CPU_CONSUMPTION_TOTAL_MAP_KEY = -2;

  private ScheduledExecutorService threadPool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(1, "CompactionMonitor");
  private long lastUpdateTime = 0L;
  // storage group name -> file level -> compacted file num
  private Map<String, Map<Integer, Integer>> compactionBeginFileCountMap = new HashMap<>();
  private Map<String, Integer> compactionBeginCountForEachSg = new HashMap<>();
  private Map<String, Map<Integer, Integer>> compactionFinishFileCountMap = new HashMap<>();
  private Map<String, Integer> compactionFinishCountForEachSg = new HashMap<>();
  // it records the total cpu time for all threads
  private long lastCpuTotalTime = 0L;
  // threadId -> cpu time
  private Map<Long, Long> cpuTimeForCompactionThread = new HashMap<>();
  private Set<Long> compactionThreadIdSet = new HashSet<>();
  private Set<Long> mergeThreadIdSet = new HashSet<>();
  private Map<Long, Long> cpuTimeForMergeThread = new HashMap<>();
  private Map<String, Pair<Integer, Integer>> mergeStartFileNumForEachSg = new HashMap<>();
  private Map<String, Integer> mergeStartCountForEachSg = new HashMap<>();
  private Map<String, Pair<Integer, Integer>> mergeFinishFileNumForEachSg = new HashMap<>();
  private Map<String, Integer> mergeFinishCountForEachSg = new HashMap<>();
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
      init();
      threadPool.scheduleWithFixedDelay(
          this::sealedMonitorStatusPeriodically,
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          IoTDBDescriptor.getInstance().getConfig().getCompactionMonitorPeriod(),
          TimeUnit.MILLISECONDS);
      LOGGER.info(
          "Start to monitor compaction, period is {} ms",
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

  private void init() {
    calculateCpuConsumptionForCompactionAndMergeThreads();
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
    saveCompactionInfo(compactionBeginCountForEachSg, compactionBeginFileCountMap, true);
    saveCompactionInfo(compactionFinishCountForEachSg, compactionFinishFileCountMap, false);
    saveMergeInfo(mergeStartCountForEachSg, mergeStartFileNumForEachSg, true);
    saveMergeInfo(mergeFinishCountForEachSg, mergeFinishFileNumForEachSg, false);
  }

  public synchronized void reportCompactionStatus(
      String storageGroupName, int compactionLevel, int fileNum, boolean begin) {
    if (!compactionThreadIdSet.contains(Thread.currentThread().getId())) {
      registerCompactionThread(Thread.currentThread().getId());
    }
    Map<String, Map<Integer, Integer>> compactionFileCountMap =
        begin ? compactionBeginFileCountMap : compactionFinishFileCountMap;
    Map<String, Integer> compactionCountMap =
        begin ? compactionBeginCountForEachSg : compactionFinishCountForEachSg;
    Map<Integer, Integer> levelFileCountMap =
        compactionFileCountMap.computeIfAbsent(storageGroupName, x -> new HashMap<>());
    int newCompactedFileCount = levelFileCountMap.getOrDefault(compactionLevel, 0) + fileNum;
    levelFileCountMap.put(compactionLevel, newCompactedFileCount);
    int newCompactionCount = compactionCountMap.getOrDefault(storageGroupName, 0) + 1;
    compactionCountMap.put(storageGroupName, newCompactionCount);
  }

  public synchronized void reportMergeStatus(
      String storageGroupName, int seqFileNum, int unseqFileNum, boolean start) {
    if (!mergeThreadIdSet.contains(Thread.currentThread().getId())) {
      registerMergeThread(Thread.currentThread().getId());
    }
    Map<String, Integer> mergeCountForSg =
        start ? mergeStartCountForEachSg : mergeFinishCountForEachSg;
    Map<String, Pair<Integer, Integer>> mergeFileNumForSg =
        start ? mergeStartFileNumForEachSg : mergeFinishFileNumForEachSg;
    mergeCountForSg.put(storageGroupName, mergeCountForSg.getOrDefault(storageGroupName, 0) + 1);
    Pair<Integer, Integer> mergeFileNumForCurrSg =
        mergeFileNumForSg.getOrDefault(storageGroupName, new Pair<>(0, 0));
    mergeFileNumForSg.put(
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
    LOGGER.info(
        "[CompactionMonitor] Total CPU time is {} ns, cpu time in last period is {} ns",
        totalCpuTime,
        lastCpuTotalTime);
    lastCpuTotalTime = totalCpuTime;
    double compactionThreadsTotalCpuConsumption = 0.0;
    // we use this map to store the cpu consumption percentage of each compaction or merge thread
    Map<Long, Double> cpuConsumptionForCompactionAndMergeThread = new HashMap<>();
    // calculate the cpu consumption of each compaction thread in this period
    // and update the total cpu time for each compaction thread
    for (long threadId : compactionThreadIdSet) {
      double cpuConsumptionForCurrentThread =
          (double)
                  (cpuTimeForCompactionThreadInThisPeriod.get(threadId)
                      - cpuTimeForCompactionThread.getOrDefault(threadId, 0L))
              / (double) (cpuTimeInThisPeriod);
      LOGGER.info(
          "[CompactionMonitor] Cpu consumption for thread {} is {}%",
          threadId, cpuConsumptionForCurrentThread * 100);
      cpuConsumptionForCompactionAndMergeThread.put(threadId, cpuConsumptionForCurrentThread);
      compactionThreadsTotalCpuConsumption += cpuConsumptionForCurrentThread;
      // update the total cpu time for each compaction thread
      cpuTimeForCompactionThread.put(
          threadId, cpuTimeForCompactionThreadInThisPeriod.get(threadId));
    }
    LOGGER.info(
        "[CompactionMonitor] cpu for compaction threads in last period is {}%",
        compactionThreadsTotalCpuConsumption * 100);

    cpuConsumptionForCompactionAndMergeThread.put(
        COMPACTION_CPU_CONSUMPTION_TOTAL_MAP_KEY, compactionThreadsTotalCpuConsumption);

    double mergeThreadsTotalCpuConsumption = 0.0;

    for (long threadId : mergeThreadIdSet) {
      double cpuConsumptionForCurrentThread =
          (double)
                  (cpuTimeForMergeThreadInThisPeriod.get(threadId)
                      - cpuTimeForMergeThread.getOrDefault(threadId, 0L))
              / (double) (cpuTimeInThisPeriod);
      cpuConsumptionForCompactionAndMergeThread.put(threadId, compactionThreadsTotalCpuConsumption);
      mergeThreadsTotalCpuConsumption += cpuConsumptionForCurrentThread;
      // update the total cpu time for each merge thread
      cpuTimeForMergeThread.put(threadId, cpuTimeForMergeThreadInThisPeriod.get(threadId));
    }

    cpuConsumptionForCompactionAndMergeThread.put(
        MERGE_CPU_CONSUMPTION_TOTAL_MAP_KEY, mergeThreadsTotalCpuConsumption);
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
          String threadName = mxBean.getThreadInfo(threadId).getThreadName();
          String[] splittedThreadName = threadName.split("-");
          int length = splittedThreadName.length;
          String measurementName =
              splittedThreadName[length - 2] + "-" + splittedThreadName[length - 1];
          compactionCpuConsumptionMeasurements.add(measurementName);
          compactionCpuConsumptionValues.add(
              Double.toString(consumptionMap.getOrDefault(threadId, 0.0)));
        }
        compactionCpuConsumptionMeasurements.add(COMPACTION_CPU_CONSUMPTION_SUM_MEASUREMENT);
        compactionCpuConsumptionValues.add(
            Double.toString(
                consumptionMap.getOrDefault(COMPACTION_CPU_CONSUMPTION_TOTAL_MAP_KEY, 0.0)));
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
          String threadName = mxBean.getThreadInfo(threadId).getThreadName();
          String[] splittedThreadName = threadName.split("-");
          int length = splittedThreadName.length;
          String measurementName =
              splittedThreadName[length - 2] + "-" + splittedThreadName[length - 1];
          mergeCpuConsumptionMeasurements.add(measurementName);
          mergeCpuConsumptionValues.add(
              Double.toString(consumptionMap.getOrDefault(threadId, 0.0)));
        }
        mergeCpuConsumptionMeasurements.add(MERGE_CPU_CONSUMPTION_SUM_MEASUREMENT);
        mergeCpuConsumptionValues.add(
            Double.toString(consumptionMap.getOrDefault(MERGE_CPU_CONSUMPTION_TOTAL_MAP_KEY, 0.0)));

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

  private void saveCompactionInfo(
      Map<String, Integer> compactionCountForSg,
      Map<String, Map<Integer, Integer>> compactionFileCountMap,
      boolean begin) {
    try {
      String[] measurementName = {"task_num"};
      for (String sgName : compactionCountForSg.keySet()) {
        PartialPath deviceName =
            new PartialPath(
                String.format(
                    begin ? COMPACTION_BEGIN_TASK_NUM_DEVICE : COMPACTION_FINISH_TASK_NUM_DEVICE,
                    sgName.replaceAll("root", "")));
        InsertRowPlan insertRowPlanForCompactionCount =
            new InsertRowPlan(
                deviceName,
                lastUpdateTime,
                measurementName,
                new String[] {Integer.toString(compactionCountForSg.get(sgName))});
        planExecutor.processNonQuery(insertRowPlanForCompactionCount);
      }
      compactionCountForSg.clear();

      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      int maxCompactionCount = Math.max(config.getSeqLevelNum(), config.getUnseqLevelNum());
      for (String sgName : compactionFileCountMap.keySet()) {
        PartialPath device =
            new PartialPath(
                String.format(
                    begin ? COMPACTION_BEGIN_FILE_NUM_DEVICE : COMPACTION_FINISH_FILE_NUM_DEVICE,
                    sgName.replaceAll("root", "")));
        Map<Integer, Integer> countMap = compactionFileCountMap.get(sgName);
        String measurementPattern = "level-%d";
        List<String> measurements = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < maxCompactionCount; ++i) {
          measurements.add(String.format(measurementPattern, i));
          values.add(Integer.toString(countMap.getOrDefault(i, 0)));
        }
        InsertRowPlan plan =
            new InsertRowPlan(
                device,
                lastUpdateTime,
                measurements.toArray(new String[0]),
                values.toArray(new String[0]));
        planExecutor.processNonQuery(plan);
      }
      compactionFileCountMap.clear();

    } catch (Throwable e) {
      LOGGER.error("[CompactionMonitor] Exception occurs while saving compaction info", e);
    }
  }

  private void saveMergeInfo(
      Map<String, Integer> mergeCountForEachSg,
      Map<String, Pair<Integer, Integer>> mergeFileNumForEachSg,
      boolean begin) {
    try {
      // save task num info
      String[] measurementName = {"task_num"};
      for (String sgName : mergeCountForEachSg.keySet()) {
        PartialPath device =
            new PartialPath(
                String.format(
                    begin ? MERGE_BEGIN_TASK_NUM_DEVICE : MERGE_FINISH_TASK_NUM_DEVICE,
                    sgName.replaceAll("root", "")));
        InsertRowPlan insertRowPlan =
            new InsertRowPlan(
                device,
                lastUpdateTime,
                measurementName,
                new String[] {Integer.toString(mergeCountForEachSg.get(sgName))});
        planExecutor.processNonQuery(insertRowPlan);
      }
      mergeCountForEachSg.clear();
      for (String sgName : mergeFileNumForEachSg.keySet()) {
        PartialPath device =
            new PartialPath(
                String.format(
                    begin ? MERGE_BEGIN_FILE_NUM_DEVICE : MERGE_FINISH_FILE_NUM_DEVICE,
                    sgName.replaceAll("root", "")));
        List<String> measurements = new ArrayList<>();
        List<String> values = new ArrayList<>();
        measurements.add("seq");
        measurements.add("unseq");
        Pair<Integer, Integer> fileNum = mergeFileNumForEachSg.get(sgName);
        values.add(Integer.toString(fileNum.left));
        values.add(Integer.toString(fileNum.right));
        InsertRowPlan plan =
            new InsertRowPlan(
                device,
                lastUpdateTime,
                measurements.toArray(new String[0]),
                values.toArray(new String[0]));
        planExecutor.processNonQuery(plan);
      }
      mergeFileNumForEachSg.clear();
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
