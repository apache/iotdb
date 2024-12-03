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

package org.apache.iotdb.db.storageengine.dataregion.wal;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.wal.allocation.ElasticStrategy;
import org.apache.iotdb.db.storageengine.dataregion.wal.allocation.FirstCreateStrategy;
import org.apache.iotdb.db.storageengine.dataregion.wal.allocation.NodeAllocationStrategy;
import org.apache.iotdb.db.storageengine.dataregion.wal.allocation.RoundRobinStrategy;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALFakeNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

/** This class is used to manage and allocate wal nodes. */
public class WALManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // manage all wal nodes and decide how to allocate them
  private final NodeAllocationStrategy walNodesManager;
  // single thread to delete old .wal files
  private ScheduledExecutorService walDeleteThread;
  // total disk usage of wal files
  private final AtomicLong totalDiskUsage = new AtomicLong();
  // total number of wal files
  private final AtomicLong totalFileNum = new AtomicLong();

  private WALManager() {
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
        || config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS_V2)) {
      walNodesManager = new FirstCreateStrategy();
    } else if (config.getMaxWalNodesNum() == 0) {
      walNodesManager = new ElasticStrategy();
    } else {
      walNodesManager = new RoundRobinStrategy(config.getMaxWalNodesNum());
    }
  }

  public static String getApplicantUniqueId(String dataRegionName, boolean sequence) {
    return config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
            || config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.IOT_CONSENSUS_V2)
        ? dataRegionName
        : dataRegionName
            + IoTDBConstant.FILE_NAME_SEPARATOR
            + (sequence ? "sequence" : "unsequence");
  }

  /** Apply for a wal node. */
  public IWALNode applyForWALNode(String applicantUniqueId) {
    if (config.getWalMode() == WALMode.DISABLE) {
      return WALFakeNode.getSuccessInstance();
    }

    return walNodesManager.applyForWALNode(applicantUniqueId);
  }

  /** WAL node will be registered only when using iot series consensus protocol. */
  public void registerWALNode(
      String applicantUniqueId, String logDirectory, long startFileVersion, long startSearchIndex) {
    if (config.getWalMode() == WALMode.DISABLE
        || (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
            && !config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.IOT_CONSENSUS_V2))) {
      return;
    }

    ((FirstCreateStrategy) walNodesManager)
        .registerWALNode(applicantUniqueId, logDirectory, startFileVersion, startSearchIndex);
    WritingMetrics.getInstance().createWALNodeInfoMetrics(applicantUniqueId);
  }

  /** WAL node will be deleted only when using iot series consensus protocol. */
  public void deleteWALNode(String applicantUniqueId) {
    if (config.getWalMode() == WALMode.DISABLE
        || (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)
            && !config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.IOT_CONSENSUS_V2))) {
      return;
    }

    ((FirstCreateStrategy) walNodesManager).deleteWALNode(applicantUniqueId);
    WritingMetrics.getInstance().removeWALNodeInfoMetrics(applicantUniqueId);
  }

  /** Region information and WAL node will be removed when using ElasticStrategy. */
  public void deleteRegionAndMayDeleteWALNode(String databaseName, String dataRegionId) {
    if (config.getWalMode() == WALMode.DISABLE || !(walNodesManager instanceof ElasticStrategy)) {
      return;
    }

    String dataRegionName = databaseName + FILE_NAME_SEPARATOR + dataRegionId;
    ((ElasticStrategy) walNodesManager)
        .deleteUniqueIdAndMayDeleteWALNode(getApplicantUniqueId(dataRegionName, true));
    ((ElasticStrategy) walNodesManager)
        .deleteUniqueIdAndMayDeleteWALNode(getApplicantUniqueId(dataRegionName, false));
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

  /** Reboot wal delete thread to hot modify delete wal period. */
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

  private void deleteOutdatedFiles() {
    // Normally, only need to delete the expired file once. When the WAL disk file size exceeds the
    // threshold, the system continues to delete expired files until the disk size is smaller than
    // the threshold.
    boolean firstLoop = true;
    while (firstLoop || shouldThrottle()) {
      deleteOutdatedFilesInWALNodes();
      if (firstLoop && shouldThrottle()) {
        logger.warn(
            "WAL disk usage {} is larger than the wal_throttle_threshold_in_byte * 0.8 {}, please check your write load, iot consensus and the pipe module. It's better to allocate more disk for WAL.",
            getTotalDiskUsage(),
            getThrottleThreshold());
      }
      firstLoop = false;
    }
  }

  protected void deleteOutdatedFilesInWALNodes() {
    if (config.getWalMode() == WALMode.DISABLE) {
      return;
    }
    List<WALNode> walNodes = walNodesManager.getNodesSnapshot();
    Map<WALNode, Long> walNode2DiskUsage = new HashMap<>();
    for (WALNode walNode : walNodes) {
      walNode2DiskUsage.put(walNode, walNode.getDiskUsage());
    }
    walNodes.sort(
        (node1, node2) -> Long.compare(walNode2DiskUsage.get(node2), walNode2DiskUsage.get(node1)));
    for (WALNode walNode : walNodes) {
      walNode.deleteOutdatedFiles();
    }
  }

  /** Wait until all write-ahead logs are flushed. */
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
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public boolean shouldThrottle() {
    return getTotalDiskUsage() >= getThrottleThreshold();
  }

  public long getThrottleThreshold() {
    return (long) (config.getThrottleThreshold() * 0.8);
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

  public void addTotalFileNum(long num) {
    totalFileNum.accumulateAndGet(num, Long::sum);
  }

  public void subtractTotalFileNum(long num) {
    totalFileNum.accumulateAndGet(num, (x, y) -> x - y);
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

  public void syncDeleteOutdatedFilesInWALNodes() {
    if (config.getWalMode() == WALMode.DISABLE || walDeleteThread == null) {
      return;
    }
    Future<?> future = walDeleteThread.submit(this::deleteOutdatedFilesInWALNodes);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw new StorageEngineFailureException("Failed to delete outdated wal file", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineFailureException("Failed to delete outdated wal file", e);
    }
  }

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
