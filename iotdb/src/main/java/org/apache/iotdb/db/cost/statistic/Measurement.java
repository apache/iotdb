/**
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
package org.apache.iotdb.db.cost.statistic;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Measurement is used to record execution time of operations defined in enum class Operation.
 * It can display average time of each operation, and proportion of operation whose execution time
 * fall into time range defined in BUCKET_IN_MS. If you want to change abscissa of histogram, just
 * change the BUCKET_IN_MS array.
 * For recording a operation, you should:
 * 1) add a item in enum class Operation.
 * 2) call <code>startTimeInNano = System.nanoTime()</code> to recode startTime of that operation.
 * 3) call <code>Measurement.INSTANCE.addOperationLatency(operation, startTimeInNano)</code> at the
 * end of that operation;
 * @see Operation
 */
public class Measurement implements MeasurementMBean, IService {

  /**
   * queue for async store time latencies.
   */
  private Queue<Long>[] operationLatenciesQueue;

  /**
   * size of each queue, this is calculated by memory.
   */
  private final long QUEUE_SIZE;

  /**
   * latencies sum of each operation.
   */
  private long[] operationLatencies;

  /**
   * the num of each operation.
   */
  private long[] operationCnt;

  /**
   * abscissa of histogram.
   */
  private final int BUCKET_IN_MS[] = {1, 4, 16, 64, 256, 1024, Integer.MAX_VALUE};

  /**
   * length of BUCKET_IN_MS.
   */
  private final int BUCKET_SIZE = BUCKET_IN_MS.length;

  /**
   * the num of operation that execution time falls into time range of BUCKET_IN_MS. The outer array
   * is each operation, the inner array is each time range in BUCKET_IN_MS.
   */
  private long[][] operationHistogram;

  /**
   * display thread and queue consumer thread.
   */
  private ScheduledExecutorService service;

  /**
   * future task of display thread and queue consumer thread.
   */
  private List<Future<?>> futureList;

  /**
   * lock for change state: start() and stopStatistic().
   */
  private ReentrantLock stateChangeLock = new ReentrantLock();

  public static final Measurement INSTANCE = AsyncMeasurementHolder.MEASUREMENT;

  private boolean isEnableStat;
  private long displayIntervalInMs;

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private final int MS_TO_NANO = 1000_000;
  private final String mbeanName = String
      .format("%s:%s=%s", "org.apache.iotdb.db.cost.statistic", IoTDBConstant.JMX_TYPE,
          getID().getJmxName());

  private Measurement() {
    IoTDBConfig tdbConfig = IoTDBDescriptor.getInstance().getConfig();
    isEnableStat = tdbConfig.isEnablePerformanceStat();
    displayIntervalInMs = tdbConfig.getPerformanceStatDisplayInterval();
    int memory_in_kb = tdbConfig.getPerformance_stat_memory_in_kb();

    QUEUE_SIZE = memory_in_kb * 1000 / Operation.values().length / 8;
    operationLatenciesQueue = new ConcurrentLinkedQueue[Operation.values().length];
    operationLatencies = new long[Operation.values().length];
    operationCnt = new long[Operation.values().length];
    for (Operation op : Operation.values()) {
      operationLatenciesQueue[op.ordinal()] = new ConcurrentLinkedQueue<>();
      operationCnt[op.ordinal()] = 0;
      operationLatencies[op.ordinal()] = 0;
    }
    operationHistogram = new long[Operation.values().length][BUCKET_SIZE];
    for (Operation operation : Operation.values()) {
      for (int i = 0; i < BUCKET_SIZE; i++) {
        operationHistogram[operation.ordinal()][i] = 0;
      }
    }

    service = IoTDBThreadPoolFactory.newScheduledThreadPool(
        2, ThreadName.TIME_COST_STATSTIC.getName());
    futureList = new ArrayList<>();
  }

  public void addOperationLatency(Operation op, long startTime) {
    if (isEnableStat && operationLatenciesQueue[op.ordinal()].size() < QUEUE_SIZE) {
      operationLatenciesQueue[op.ordinal()].add((System.nanoTime() - startTime));
    }
  }

  @Override
  public void startContinuousStatistics() {
    stateChangeLock.lock();
    try {
      if (isEnableStat) {
        return;
      }
      isEnableStat = true;
      futureList.clear();
      Future future = service.scheduleWithFixedDelay(
          new Measurement.DisplayRunnable(), 20, displayIntervalInMs, TimeUnit.MILLISECONDS);
      futureList.add(future);
      futureList.add(service.schedule(new QueueConsumerThread(), 10, TimeUnit.MILLISECONDS));


    } catch (Exception e) {
      LOGGER.error("Find error when start performance statistic thread, because {}", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void startOneTimeStatistics(){
    stateChangeLock.lock();
    try {
      if (isEnableStat) {
        return;
      }
      isEnableStat = true;
      futureList.clear();
      futureList.add(service.schedule(new QueueConsumerThread(), 10, TimeUnit.MILLISECONDS));
      Future future = service.schedule(()->{
        showMeasurements();
          stopStatistic();
          }, displayIntervalInMs, TimeUnit.MILLISECONDS);
      futureList.add(future);
    } catch (Exception e) {
      LOGGER.error("Find error when start performance statistic thread, because {}", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void stopStatistic() {
    stateChangeLock.lock();
    try {
      if(isEnableStat == false){
        return;
      }
      isEnableStat = false;
      for (Future future : futureList) {
        if (future != null) {
          future.cancel(true);

        }
      }
    } catch (Exception e) {
      LOGGER.error("Find error when stopStatistic time cost statstic thread, because {}", e);
    } finally {
      stateChangeLock.unlock();
    }

  }

  /**
   * start service.
   */
  @Override
  public void start() throws StartupException {
    // start display thread and consumer threads.
    if (isEnableStat) {
      Future future = service.scheduleWithFixedDelay(
          new Measurement.DisplayRunnable(), 20, displayIntervalInMs, TimeUnit.MILLISECONDS);
      futureList.add(future);
      futureList.add(service.schedule(new QueueConsumerThread(), 10, TimeUnit.MILLISECONDS));

    }
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage, e);
    }
  }

  /**
   * stop service.
   */
  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
    if (service == null || service.isShutdown()) {
      return;
    }
    stopStatistic();
    futureList.clear();
    service.shutdownNow();
    try {
      service.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Performance statistic service could not be shutdown, {}", e.getMessage());
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PERFORMANCE_STATISTIC_SERVICE;
  }

  @Override
  public boolean isEnableStat() {
    return isEnableStat;
  }

  @Override
  public void setEnableStat(boolean enableStat) {
    isEnableStat = enableStat;
  }

  public long getDisplayIntervalInMs() {
    return displayIntervalInMs;
  }

  public void setDisplayIntervalInMs(long displayIntervalInMs) {
    this.displayIntervalInMs = displayIntervalInMs;
  }

  private static class AsyncMeasurementHolder {

    private static final Measurement MEASUREMENT = new Measurement();

    private AsyncMeasurementHolder() {
    }
  }

  private void showMeasurements() {
    Date date = new Date();
    LOGGER.info(
        "====================================={} Measurement (ms)======================================",
        date.toString());
    String head = String
        .format("%-45s%-25s%-25s%-25s", "OPERATION", "COUNT", "TOTAL_TIME", "AVG_TIME");
    LOGGER.info(head);
    for (Operation operation : Operation.values()) {
      long cnt = operationCnt[operation.ordinal()];
      long totalInMs = 0;
      totalInMs = operationLatencies[operation.ordinal()] / 1000000;
      String avg = String.format("%.4f", (totalInMs / (cnt + 1e-9)));
      String item = String
          .format("%-45s%-25s%-25s%-25s", operation.name, cnt + "", totalInMs + "", avg);
      LOGGER.info(item);
    }
    LOGGER.info(
        "==========================================OPERATION HISTOGRAM====================================================");
    String histogramHead = String.format("%-45s", "OPERATION");
    for (int i = 0; i < BUCKET_SIZE; i++) {
      histogramHead += String.format("%-8s", BUCKET_IN_MS[i] + "ms");
    }
    LOGGER.info(histogramHead);
    for (Operation operation : Operation.values()) {
      String item = String.format("%-45s", operation.getName());
      long cnt = operationCnt[operation.ordinal()];
      for (int i = 0; i < BUCKET_SIZE; i++) {
        String avg = String
            .format("%.2f", (operationHistogram[operation.ordinal()][i] / (cnt + 1e-9) * 100));
        item += String.format("%-8s", avg + "%");
      }
      LOGGER.info(item);
    }

    LOGGER.info(
        "=================================================================================================================");
  }

  class DisplayRunnable implements Runnable {

    @Override
    public void run() {
      showMeasurements();
    }
  }

  class QueueConsumerThread implements Runnable {

    @Override
    public void run() {
      consumer();
    }

    private void consumer(){
      int cnt = 0;
      boolean allEmpty = false;
      while (true) {
        cnt++;
        if (cnt > 2 * QUEUE_SIZE || allEmpty) {
          try {
            Thread.sleep(1000);
            cnt = 0;
            allEmpty = false;
            continue;
          } catch (InterruptedException e) {
            return;
          }
        }
        allEmpty = true;
        for (Operation op : Operation.values()) {
          int idx = op.ordinal();
          Queue<Long> queue = operationLatenciesQueue[idx];
          Long time = queue.poll();
          if (time != null) {
            operationLatencies[idx] += time;
            operationCnt[idx]++;
            operationHistogram[idx][calIndex(time)]++;
            allEmpty = false;
          }
        }
      }
    }
  }
  private int calIndex(long x) {
    x /= MS_TO_NANO;
    for (int i = 0; i < MS_TO_NANO; i++) {
      if (BUCKET_IN_MS[i] >= x) {
        return i;
      }
    }
    return BUCKET_SIZE - 1;
  }

}
