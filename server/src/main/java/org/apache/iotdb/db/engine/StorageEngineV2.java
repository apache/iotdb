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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.ThreadUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageEngineV2 implements IService {
  private static final Logger logger = LoggerFactory.getLogger(StorageEngineV2.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;

  /**
   * Time range for dividing storage group, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  @ServerConfigConsistent private static long timePartitionInterval = -1;
  /** whether enable data partition if disabled, all data belongs to partition 0 */
  @ServerConfigConsistent private static boolean enablePartition = config.isEnablePartition();

  private final boolean enableMemControl = config.isEnableMemControl();

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir =
      FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";

  /** DataRegionId -> DataRegion */
  private final ConcurrentHashMap<ConsensusGroupId, VirtualStorageGroupProcessor> dataRegionMap =
      new ConcurrentHashMap<>();

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);

  private ScheduledExecutorService ttlCheckThread;
  private ScheduledExecutorService seqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService unseqMemtableTimedFlushCheckThread;

  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();
  private ExecutorService recoveryThreadPool;
  // add customized listeners here for flush and close events
  private List<CloseFileListener> customCloseFileListeners = new ArrayList<>();
  private List<FlushListener> customFlushListeners = new ArrayList<>();

  private StorageEngineV2() {}

  public static StorageEngineV2 getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static void initTimePartition() {
    timePartitionInterval =
        convertMilliWithPrecision(
            IoTDBDescriptor.getInstance().getConfig().getPartitionInterval() * 1000L);
  }

  public static long convertMilliWithPrecision(long milliTime) {
    long result = milliTime;
    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    return result;
  }

  public static long getTimePartitionInterval() {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return timePartitionInterval;
  }

  @TestOnly
  public static void setTimePartitionInterval(long timePartitionInterval) {
    StorageEngineV2.timePartitionInterval = timePartitionInterval;
  }

  public static long getTimePartition(long time) {
    return enablePartition ? time / timePartitionInterval : 0;
  }

  public static boolean isEnablePartition() {
    return enablePartition;
  }

  @TestOnly
  public static void setEnablePartition(boolean enablePartition) {
    StorageEngineV2.enablePartition = enablePartition;
  }

  /** block insertion if the insertion is rejected by memory control */
  public static void blockInsertionIfReject(TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException {
    long startTime = System.currentTimeMillis();
    while (SystemInfo.getInstance().isRejected()) {
      if (tsFileProcessor != null && tsFileProcessor.shouldFlush()) {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startTime > config.getMaxWaitingTimeWhenInsertBlocked()) {
          throw new WriteProcessRejectException(
              "System rejected over " + (System.currentTimeMillis() - startTime) + "ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isAllSgReady() {
    return isAllSgReady.get();
  }

  public void setAllSgReady(boolean allSgReady) {
    isAllSgReady.set(allSgReady);
  }

  public void recover() {
    setAllSgReady(false);
    recoveryThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");
    try {
      getLocalDataRegion();
    } catch (Exception e) {
      throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
    }
    List<Future<Void>> futures = new LinkedList<>();
    asyncRecover(recoveryThreadPool, futures);

    // operations after all virtual storage groups are recovered
    Thread recoverEndTrigger =
        new Thread(
            () -> {
              for (Future<Void> future : futures) {
                try {
                  future.get();
                } catch (ExecutionException e) {
                  throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
                }
              }
              recoveryThreadPool.shutdown();
              setAllSgReady(true);
            });
    recoverEndTrigger.start();
  }

  private void getLocalDataRegion() throws MetadataException, StorageGroupProcessorException {
    File system = SystemFileFactory.INSTANCE.getFile(systemDir);
    File[] sgDirs = system.listFiles();
    for (File sgDir : sgDirs) {
      if (!sgDir.isDirectory()) {
        continue;
      }
      String sg = sgDir.getName();
      // TODO: need to get TTL Info from config node
      long ttl = Integer.MAX_VALUE;
      for (File dataRegionDir : sgDir.listFiles()) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        ConsensusGroupId dataRegionId = new DataRegionId(Integer.parseInt(dataRegionDir.getName()));
        VirtualStorageGroupProcessor dataRegion =
            buildNewStorageGroupProcessor(sg, dataRegionDir.getName(), ttl);
        dataRegionMap.putIfAbsent(dataRegionId, dataRegion);
      }
    }
  }

  private void asyncRecover(ExecutorService pool, List<Future<Void>> futures) {
    for (VirtualStorageGroupProcessor processor : dataRegionMap.values()) {
      Callable<Void> recoverVsgTask =
          () -> {
            processor.setReady(true);
            return null;
          };
      futures.add(pool.submit(recoverVsgTask));
    }
  }

  @Override
  public void start() {
    // build time Interval to divide time partition
    if (!enablePartition) {
      timePartitionInterval = Long.MAX_VALUE;
    } else {
      initTimePartition();
    }

    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }

    // recover upgrade process
    UpgradeUtils.recoverUpgrade();

    recover();

    ttlCheckThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("TTL-Check");
    ttlCheckThread.scheduleAtFixedRate(
        this::checkTTL, TTL_CHECK_INTERVAL, TTL_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    logger.info("start ttl check thread successfully.");

    startTimedService();
  }

  private void checkTTL() {
    try {
      for (VirtualStorageGroupProcessor dataRegion : dataRegionMap.values()) {
        if (dataRegion != null) {
          dataRegion.checkFilesTTL();
        }
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("An error occurred when checking TTL", e);
    }
  }

  private void startTimedService() {
    // timed flush sequence memtable
    if (config.isEnableTimedFlushSeqMemtable()) {
      seqMemtableTimedFlushCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.TIMED_FlUSH_SEQ_MEMTABLE.getName());
      seqMemtableTimedFlushCheckThread.scheduleAtFixedRate(
          this::timedFlushSeqMemTable,
          config.getSeqMemtableFlushCheckInterval(),
          config.getSeqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start sequence memtable timed flush check thread successfully.");
    }
    // timed flush unsequence memtable
    if (config.isEnableTimedFlushUnseqMemtable()) {
      unseqMemtableTimedFlushCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.TIMED_FlUSH_UNSEQ_MEMTABLE.getName());
      unseqMemtableTimedFlushCheckThread.scheduleAtFixedRate(
          this::timedFlushUnseqMemTable,
          config.getUnseqMemtableFlushCheckInterval(),
          config.getUnseqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start unsequence memtable timed flush check thread successfully.");
    }
  }

  private void timedFlushSeqMemTable() {
    try {
      for (VirtualStorageGroupProcessor dataRegion : dataRegionMap.values()) {
        if (dataRegion != null) {
          dataRegion.timedFlushSeqMemTable();
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing sequence memtables", e);
    }
  }

  private void timedFlushUnseqMemTable() {
    try {
      for (VirtualStorageGroupProcessor dataRegion : dataRegionMap.values()) {
        if (dataRegion != null) {
          dataRegion.timedFlushUnseqMemTable();
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing unsequence memtables", e);
    }
  }

  @Override
  public void stop() {
    for (VirtualStorageGroupProcessor vsg : dataRegionMap.values()) {
      if (vsg != null) {
        ThreadUtils.stopThreadPool(
            vsg.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
      }
    }
    syncCloseAllProcessor();
    ThreadUtils.stopThreadPool(ttlCheckThread, ThreadName.TTL_CHECK_SERVICE);
    ThreadUtils.stopThreadPool(
        seqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_SEQ_MEMTABLE);
    ThreadUtils.stopThreadPool(
        unseqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_UNSEQ_MEMTABLE);
    recoveryThreadPool.shutdownNow();
    dataRegionMap.clear();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
        ThreadUtils.stopThreadPool(
            virtualStorageGroupProcessor.getTimedCompactionScheduleTask(),
            ThreadName.COMPACTION_SCHEDULE);
      }
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    shutdownTimedService(ttlCheckThread, "TTlCheckThread");
    shutdownTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    shutdownTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    recoveryThreadPool.shutdownNow();
    dataRegionMap.clear();
  }

  private void shutdownTimedService(ScheduledExecutorService pool, String poolName) {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 30s", poolName);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void stopTimedServiceAndThrow(ScheduledExecutorService pool, String poolName)
      throws ShutdownException {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("{} still doesn't exit after 30s", poolName);
        throw new ShutdownException(e);
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public VirtualStorageGroupProcessor buildNewStorageGroupProcessor(
      String logicalStorageGroupName, String virtualStorageGroupId, long ttl)
      throws StorageGroupProcessorException {
    VirtualStorageGroupProcessor processor;
    logger.info(
        "construct a processor instance, the storage group is {}, Thread is {}",
        logicalStorageGroupName,
        Thread.currentThread().getId());
    processor =
        new VirtualStorageGroupProcessor(
            systemDir + File.separator + logicalStorageGroupName,
            virtualStorageGroupId,
            fileFlushPolicy,
            logicalStorageGroupName);
    processor.setDataTTL(ttl);
    processor.setCustomFlushListeners(customFlushListeners);
    processor.setCustomCloseFileListeners(customCloseFileListeners);
    return processor;
  }

  /** This function is just for unit test. */
  @TestOnly
  public synchronized void reset() {
    dataRegionMap.clear();
  }

  /**
   * insert an InsertRowNode to a storage group.
   *
   * @param insertRowNode
   */
  // TODO:(New insert)
  public void insert(ConsensusGroupId dataRegionId, InsertRowNode insertRowNode)
      throws StorageEngineException, MetadataException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }

    VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);

    try {
      dataRegion.insert(insertRowNode);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /** insert an InsertTabletNode to a storage group */
  // TODO:(New insert)
  public void insertTablet(ConsensusGroupId dataRegionId, InsertTabletNode insertTabletNode)
      throws StorageEngineException, BatchProcessException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }
    VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);
    dataRegion.insertTablet(insertTabletNode);
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
      if (virtualStorageGroupProcessor != null) {
        virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all storage group processor");
    for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
      if (virtualStorageGroupProcessor != null) {
        virtualStorageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
      }
    }
  }

  public void setTTL(List<ConsensusGroupId> dataRegionIdList, long dataTTL) {
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);
      if (dataRegion != null) {
        dataRegion.setDataTTL(dataTTL);
      }
    }
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  /**
   * Get a map indicating which storage groups have working TsFileProcessors and its associated
   * partitionId and whether it is sequence or not.
   *
   * @return storage group -> a list of partitionId-isSequence pairs
   */
  public Map<String, List<Pair<Long, Boolean>>> getWorkingStorageGroupPartitions() {
    Map<String, List<Pair<Long, Boolean>>> res = new ConcurrentHashMap<>();
    for (Entry<ConsensusGroupId, VirtualStorageGroupProcessor> entry : dataRegionMap.entrySet()) {
      VirtualStorageGroupProcessor virtualStorageGroupProcessor = entry.getValue();
      if (virtualStorageGroupProcessor != null) {
        List<Pair<Long, Boolean>> partitionIdList = new ArrayList<>();
        for (TsFileProcessor tsFileProcessor :
            virtualStorageGroupProcessor.getWorkSequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), true);
          partitionIdList.add(tmpPair);
        }

        for (TsFileProcessor tsFileProcessor :
            virtualStorageGroupProcessor.getWorkUnsequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), false);
          partitionIdList.add(tmpPair);
        }

        res.put(virtualStorageGroupProcessor.getStorageGroupPath(), partitionIdList);
      }
    }

    return res;
  }

  /**
   * Add a listener to listen flush start/end events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerFlushListener(FlushListener listener) {
    customFlushListeners.add(listener);
  }

  /**
   * Add a listener to listen file close events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerCloseFileListener(CloseFileListener listener) {
    customCloseFileListeners.add(listener);
  }

  // When registering a new region, the coordinator needs to register the corresponding region with
  // the local engine before adding the corresponding consensusGroup to the consensus layer
  public VirtualStorageGroupProcessor createDataRegion(DataRegionId regionId, String sg, long ttl)
      throws StorageEngineException {
    try {
      VirtualStorageGroupProcessor dataRegion =
          buildNewStorageGroupProcessor(sg, regionId.toString(), ttl);
      dataRegionMap.put(regionId, dataRegion);
    } catch (StorageGroupProcessorException e) {
      throw new StorageEngineException(e);
    }
    return null;
  }

  public VirtualStorageGroupProcessor getDataRegion(DataRegionId regionId) {
    return dataRegionMap.get(regionId);
  }

  static class InstanceHolder {

    private static final StorageEngineV2 INSTANCE = new StorageEngineV2();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
