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
package org.apache.iotdb.db.wal;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.db.wal.allocation.ElasticStrategy;
import org.apache.iotdb.db.wal.allocation.FirstCreateStrategy;
import org.apache.iotdb.db.wal.allocation.NodeAllocationStrategy;
import org.apache.iotdb.db.wal.allocation.RoundRobinStrategy;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.node.WALFakeNode;
import org.apache.iotdb.db.wal.node.WALNode;
import org.apache.iotdb.db.wal.utils.WALMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** This class is used to manage and allocate wal nodes */
public class WALManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** manage all wal nodes and decide how to allocate them */
  private final NodeAllocationStrategy walNodesManager;
  /** single thread to delete old .wal files */
  private ScheduledExecutorService walDeleteThread;
  /** total disk usage of wal files */
  private final AtomicLong totalDiskUsage = new AtomicLong();
  /** total number of wal files */
  private final AtomicLong totalFileNum = new AtomicLong();

  private WALManager() {
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      walNodesManager = new FirstCreateStrategy();
    } else if (config.getMaxWalNodesNum() == 0) {
      walNodesManager = new ElasticStrategy();
    } else {
      walNodesManager = new RoundRobinStrategy(config.getMaxWalNodesNum());
    }
  }

  public static String getApplicantUniqueId(String storageGroupName, boolean sequence) {
    return config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
        ? storageGroupName
        : storageGroupName
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + (sequence ? "sequence" : "unsequence");
  }

  /** Apply for a wal node */
  public IWALNode applyForWALNode(String applicantUniqueId) {
    if (config.getWalMode() == WALMode.DISABLE) {
      return WALFakeNode.getSuccessInstance();
    }

    return walNodesManager.applyForWALNode(applicantUniqueId);
  }

  /** WAL node will be registered only when using iot consensus protocol */
  public void registerWALNode(
      String applicantUniqueId, String logDirectory, long startFileVersion, long startSearchIndex) {
    if (config.getWalMode() == WALMode.DISABLE
        || !config.isClusterMode()
        || !config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      return;
    }

    ((FirstCreateStrategy) walNodesManager)
        .registerWALNode(applicantUniqueId, logDirectory, startFileVersion, startSearchIndex);
    WritingMetricsManager.getInstance().createWALNodeInfoMetrics(applicantUniqueId);
  }

  /** WAL node will be deleted only when using iot consensus protocol */
  public void deleteWALNode(String applicantUniqueId) {
    if (config.getWalMode() == WALMode.DISABLE
        || !config.isClusterMode()
        || !config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
      return;
    }

    ((FirstCreateStrategy) walNodesManager).deleteWALNode(applicantUniqueId);
    WritingMetricsManager.getInstance().removeWALNodeInfoMetrics(applicantUniqueId);
  }

  @Override
  public void start() throws StartupException {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    try {
      registerScheduleTask(
          config.getDeleteWalFilesPeriodInMs(), config.getDeleteWalFilesPeriodInMs());
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** reboot wal delete thread to hot modify delete wal period */
  public void rebootWALDeleteThread() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    logger.info("Start rebooting wal delete thread.");
    if (walDeleteThread != null) {
      shutdownThread(walDeleteThread, ThreadName.WAL_DELETE);
    }
    logger.info("Stop wal delete thread successfully, and now restart it.");
    registerScheduleTask(0, config.getDeleteWalFilesPeriodInMs());
    logger.info(
        "Reboot wal delete thread successfully, current period is {} ms",
        config.getDeleteWalFilesPeriodInMs());
  }

  /** submit delete outdated wal files task and wait for result */
  public void deleteOutdatedWALFiles() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    if (walDeleteThread == null) {
      return;
    }

    Future<?> future = walDeleteThread.submit(this::deleteOutdatedFiles);
    try {
      future.get();
    } catch (ExecutionException e) {
      logger.warn("Exception occurs when deleting wal files", e);
    } catch (InterruptedException e) {
      logger.warn("Interrupted when deleting wal files", e);
      Thread.currentThread().interrupt();
    }
  }

  private void deleteOutdatedFiles() {
    for (WALNode walNode : walNodesManager.getNodesSnapshot()) {
      walNode.deleteOutdatedFiles();
    }
  }

  /** Wait until all write-ahead logs are flushed */
  public void waitAllWALFlushed() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    for (WALNode walNode : walNodesManager.getNodesSnapshot()) {
      while (!walNode.isAllWALEntriesConsumed()) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          logger.error("Interrupted when waiting for all write-ahead logs flushed.");
        }
      }
    }
  }

  public long getTotalDiskUsage() {
    return totalDiskUsage.get();
  }

  public long getWALNodesNum() {
    return walNodesManager.getNodesNum();
  }

  public void addTotalDiskUsage(long size) {
    totalDiskUsage.accumulateAndGet(size, Long::sum);
  }

  public void subtractTotalDiskUsage(long size) {
    totalDiskUsage.accumulateAndGet(size, (x, y) -> x - y);
  }

  public long getTotalFileNum() {
    return totalFileNum.get();
  }

  public void addTotalFileNum(long size) {
    totalFileNum.accumulateAndGet(size, Long::sum);
  }

  public void subtractTotalFileNum(long size) {
    totalFileNum.accumulateAndGet(size, (x, y) -> x - y);
  }

  @Override
  public void stop() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }

    if (walDeleteThread != null) {
      shutdownThread(walDeleteThread, ThreadName.WAL_DELETE);
      walDeleteThread = null;
    }
    clear();
  }

  private void shutdownThread(ExecutorService thread, ThreadName threadName) {
    thread.shutdown();
    try {
      if (!thread.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Waiting thread {} to be terminated is timeout", threadName.getName());
      }
    } catch (InterruptedException e) {
      logger.warn("Thread {} still doesn't exit after 30s", threadName.getName());
      Thread.currentThread().interrupt();
    }
  }

  private void registerScheduleTask(long initDelayMs, long periodMs) {
    walDeleteThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(ThreadName.WAL_DELETE.getName());
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        walDeleteThread, this::deleteOutdatedFiles, initDelayMs, periodMs, TimeUnit.MILLISECONDS);
  }

  @TestOnly
  public void clear() {
    totalDiskUsage.set(0);
    walNodesManager.clear();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  public static WALManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final WALManager INSTANCE = new WALManager();
  }
}
