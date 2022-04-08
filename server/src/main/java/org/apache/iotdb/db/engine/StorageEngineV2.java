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
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IoTDB;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
  private ScheduledExecutorService tsFileTimedCloseCheckThread;

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

  public static TimePartitionSlot getTimePartitionSlot(long time) {
    TimePartitionSlot timePartitionSlot = new TimePartitionSlot();
    if (enablePartition) {
      timePartitionSlot.setStartTime(time - time % timePartitionInterval);
    } else {
      timePartitionSlot.setStartTime(0);
    }
    return timePartitionSlot;
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
      PartialPath sg = new PartialPath(sgDir.getName());
      // TODO: need to get TTL Info from config node
      for (File dataRegionDir : sgDir.listFiles()) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        ConsensusGroupId dataRegionId =
            new ConsensusGroupId(GroupType.DataRegion, Integer.parseInt(dataRegionDir.getName()));
        VirtualStorageGroupProcessor dataRegion =
            buildNewStorageGroupProcessor(sg, dataRegionDir.getName());
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
    // timed close tsfile
    if (config.isEnableTimedCloseTsFile()) {
      tsFileTimedCloseCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.TIMED_CLOSE_TSFILE.getName());
      tsFileTimedCloseCheckThread.scheduleAtFixedRate(
          this::timedCloseTsFileProcessor,
          config.getCloseTsFileCheckInterval(),
          config.getCloseTsFileCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start tsfile timed close check thread successfully.");
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

  private void timedCloseTsFileProcessor() {
    try {
      for (VirtualStorageGroupProcessor dataRegion : dataRegionMap.values()) {
        if (dataRegion != null) {
          dataRegion.timedCloseTsFileProcessor();
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed closing tsfiles interval", e);
    }
  }

  @Override
  public void stop() {
    for (VirtualStorageGroupProcessor vsg : dataRegionMap.values()) {
      if (vsg != null) {
        ThreadUtils.stopThreadPool(
            vsg.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
        ThreadUtils.stopThreadPool(vsg.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
      }
    }
    syncCloseAllProcessor();
    ThreadUtils.stopThreadPool(ttlCheckThread, ThreadName.TTL_CHECK_SERVICE);
    ThreadUtils.stopThreadPool(
        seqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_SEQ_MEMTABLE);
    ThreadUtils.stopThreadPool(
        unseqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_UNSEQ_MEMTABLE);
    ThreadUtils.stopThreadPool(tsFileTimedCloseCheckThread, ThreadName.TIMED_CLOSE_TSFILE);
    recoveryThreadPool.shutdownNow();
    // TODO(Removed from new wal)
    //    for (PartialPath storageGroup : IoTDB.schemaEngine.getAllStorageGroupPaths()) {
    //      this.releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroup);
    //    }
    dataRegionMap.clear();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
        ThreadUtils.stopThreadPool(
            virtualStorageGroupProcessor.getTimedCompactionScheduleTask(),
            ThreadName.COMPACTION_SCHEDULE);
        ThreadUtils.stopThreadPool(
            virtualStorageGroupProcessor.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
      }
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    shutdownTimedService(ttlCheckThread, "TTlCheckThread");
    shutdownTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    shutdownTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    shutdownTimedService(tsFileTimedCloseCheckThread, "TsFileTimedCloseCheckThread");
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

  /** reboot timed flush sequence/unsequence memetable thread, timed close tsfile thread */
  public void rebootTimedService() throws ShutdownException {
    logger.info("Start rebooting all timed service.");

    // exclude ttl check thread
    stopTimedServiceAndThrow(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(
        unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(tsFileTimedCloseCheckThread, "TsFileTimedCloseCheckThread");

    logger.info("Stop all timed service successfully, and now restart them.");

    startTimedService();

    logger.info("Reboot all timed service successfully");
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
   * This method is for create new DataRegion in StorageEngine
   *
   * @param dataRegionId
   * @param logicalStorageGroupName
   */
  public void createNewDataRegion(
      ConsensusGroupId dataRegionId, PartialPath logicalStorageGroupName)
      throws StorageEngineException {
    try {
      VirtualStorageGroupProcessor dataRegion =
          buildNewStorageGroupProcessor(logicalStorageGroupName, dataRegionId.toString());
      dataRegionMap.put(dataRegionId, dataRegion);
    } catch (StorageGroupProcessorException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public VirtualStorageGroupProcessor buildNewStorageGroupProcessor(
      PartialPath logicalStorageGroupName, String virtualStorageGroupId)
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
            logicalStorageGroupName.getFullPath());
    // TODO: set TTL
    // processor.setDataTTL(storageGroupMNode.getDataTTL());
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
  //  // TODO:(New insert)
  //  public void insertV2(DataRegionId dataRegionId, InsertRowNode insertRowNode)
  //      throws StorageEngineException, MetadataException {
  //    if (enableMemControl) {
  //      try {
  //        blockInsertionIfReject(null);
  //      } catch (WriteProcessException e) {
  //        throw new StorageEngineException(e);
  //      }
  //    }
  //
  //    VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);
  //
  //    try {
  //      dataRegion.insert(insertRowNode);
  //    } catch (WriteProcessException e) {
  //      throw new StorageEngineException(e);
  //    }
  //  }

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

  public void closeStorageGroupProcessor(
      List<ConsensusGroupId> dataRegionIdList, boolean isSeq, boolean isSync) {

    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor processor = dataRegionMap.get(dataRegionId);
      if (processor == null) {
        continue;
      }

      if (logger.isInfoEnabled()) {
        logger.info(
            "{} closing sg processor is called for closing {}, seq = {}",
            isSync ? "sync" : "async",
            processor.getVirtualStorageGroupId() + "-" + processor.getLogicalStorageGroupName(),
            isSeq);
      }

      processor.writeLock("VirtualCloseStorageGroupProcessor-204");
      try {
        if (isSeq) {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor :
              new ArrayList<>(processor.getWorkSequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(true, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(true, tsfileProcessor);
            }
          }
        } else {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor :
              new ArrayList<>(processor.getWorkUnsequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(false, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(false, tsfileProcessor);
            }
          }
        }
      } finally {
        processor.writeUnlock();
      }
    }
  }

  /**
   * @param dataRegionIdList list of dataRegionId
   * @param partitionId the partition id
   * @param isSeq is sequence tsfile or unsequence tsfile
   * @param isSync close tsfile synchronously or asynchronously
   * @throws StorageGroupNotSetException
   */
  public void closeStorageGroupProcessor(
      List<ConsensusGroupId> dataRegionIdList, long partitionId, boolean isSeq, boolean isSync) {

    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor processor = dataRegionMap.get(dataRegionId);

      if (processor != null) {
        logger.info(
            "async closing sg processor is called for closing {}, seq = {}, partitionId = {}",
            processor.getVirtualStorageGroupId() + "-" + processor.getLogicalStorageGroupName(),
            isSeq,
            partitionId);
        processor.writeLock("VirtualCloseStorageGroupProcessor-242");
        try {
          // to avoid concurrent modification problem, we need a new array list
          List<TsFileProcessor> processors =
              isSeq
                  ? new ArrayList<>(processor.getWorkSequenceTsFileProcessors())
                  : new ArrayList<>(processor.getWorkUnsequenceTsFileProcessors());
          for (TsFileProcessor tsfileProcessor : processors) {
            if (tsfileProcessor.getTimeRangeId() == partitionId) {
              if (isSync) {
                processor.syncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              } else {
                processor.asyncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              }
              break;
            }
          }
        } finally {
          processor.writeUnlock();
        }
      }
    }
  }

  public void delete(
      List<ConsensusGroupId> dataRegionIdList,
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      for (ConsensusGroupId dataRegionId : dataRegionIdList) {
        VirtualStorageGroupProcessor processor = dataRegionMap.get(dataRegionId);
        if (processor != null) {
          processor.delete(path, startTime, endTime, planIndex, timePartitionFilter);
        }
      }
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** delete data of timeseries "{deviceId}.{measurementId}" */
  public void deleteTimeseries(
      List<ConsensusGroupId> dataRegionIdList,
      PartialPath path,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      for (ConsensusGroupId dataRegionId : dataRegionIdList) {
        // storage group has no data
        if (!dataRegionMap.containsKey(dataRegionId)) {
          continue;
        }
        VirtualStorageGroupProcessor virtualStorageGroupProcessor = dataRegionMap.get(dataRegionId);
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.delete(
              path, Long.MIN_VALUE, Long.MAX_VALUE, planIndex, timePartitionFilter);
        }
      }
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /**
   * count all Tsfiles which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded
   */
  public int countUpgradeFiles() {
    int totalUpgradeFileNum = 0;
    for (VirtualStorageGroupProcessor processor : dataRegionMap.values()) {
      if (processor != null) {
        totalUpgradeFileNum += processor.countUpgradeFiles();
      }
    }
    return totalUpgradeFileNum;
  }

  /**
   * upgrade all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void upgradeAll() throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException(
          "Current system mode is read only, does not support file upgrade");
    }
    for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
      if (virtualStorageGroupProcessor != null) {
        virtualStorageGroupProcessor.upgrade();
      }
    }
  }

  public void getResourcesToBeSettled(
      List<ConsensusGroupId> dataRegionIdList,
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths)
      throws StorageEngineException {

    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor virtualStorageGroupProcessor = dataRegionMap.get(dataRegionId);
      if (virtualStorageGroupProcessor != null) {
        if (!virtualStorageGroupProcessor.getIsSettling().compareAndSet(false, true)) {
          throw new StorageEngineException(
              "Storage Group "
                  + virtualStorageGroupProcessor.getStorageGroupPath()
                  + " is already being settled now.");
        }
        virtualStorageGroupProcessor.addSettleFilesToList(
            seqResourcesToBeSettled, unseqResourcesToBeSettled, tsFilePaths);
      }
    }
  }

  public void setSettling(List<ConsensusGroupId> dataRegionIdList, boolean isSettling) {
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor virtualStorageGroupProcessor = dataRegionMap.get(dataRegionId);
      if (virtualStorageGroupProcessor != null) {
        virtualStorageGroupProcessor.setSettling(isSettling);
      }
    }
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll() throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionMap.values()) {
      if (virtualStorageGroupProcessor != null) {
        virtualStorageGroupProcessor.compact();
      }
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneDataRegion(ConsensusGroupId dataRegionId) {
    if (dataRegionMap.containsKey(dataRegionId)) {
      syncDeleteDataFiles(dataRegionId);
    }
  }

  private void syncDeleteDataFiles(ConsensusGroupId dataRegionId) {
    logger.info("Force to delete the data in data region {}", dataRegionId);
    VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);
    if (dataRegion != null) {
      dataRegion.syncDeleteDataFiles();
    }
  }

  /** delete all data of storage groups' timeseries. */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (ConsensusGroupId dataRegionId : dataRegionMap.keySet()) {
      this.deleteAllDataFilesInOneDataRegion(dataRegionId);
    }
    return true;
  }

  public void setTTL(List<ConsensusGroupId> dataRegionIdList, long dataTTL) {
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      VirtualStorageGroupProcessor dataRegion = dataRegionMap.get(dataRegionId);
      if (dataRegion != null) {
        dataRegion.setDataTTL(dataTTL);
      }
    }
  }

  public void deleteStorageGroup(List<ConsensusGroupId> dataRegionIdList) {
    // TODO:should be removed by new wal
    // releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroupPath);
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      deleteAllDataFilesInOneDataRegion(dataRegionId);
      VirtualStorageGroupProcessor dataRegion = dataRegionMap.remove(dataRegionId);
      dataRegion.deleteFolder(systemDir + File.pathSeparator + dataRegion.getStorageGroupPath());
      ThreadUtils.stopThreadPool(
          dataRegion.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
      ThreadUtils.stopThreadPool(dataRegion.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
    }
  }

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @param needCheck check if the tsfile is an internal TsFile. If you make sure it is inside, no
   *     need to check
   * @return sg name
   * @throws IllegalPathException throw if tsfile is not an internal TsFile
   */
  public String getSgByEngineFile(File file, boolean needCheck) throws IllegalPathException {
    if (needCheck) {
      File dataDir =
          file.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile();
      if (dataDir.exists()) {
        String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
        for (String dir : dataDirs) {
          try {
            if (Files.isSameFile(Paths.get(dir), dataDir.toPath())) {
              return file.getParentFile().getParentFile().getParentFile().getName();
            }
          } catch (IOException e) {
            throw new IllegalPathException(file.getAbsolutePath(), e.getMessage());
          }
        }
      }
      throw new IllegalPathException(file.getAbsolutePath(), "it's not an internal tsfile.");
    } else {
      return file.getParentFile().getParentFile().getParentFile().getName();
    }
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  /**
   * Set the version of given partition to newMaxVersion if it is larger than the current version.
   */
  public void setPartitionVersionToMax(
      List<ConsensusGroupId> dataRegionIdList, long partitionId, long newMaxVersion) {
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      dataRegionMap.get(dataRegionId).setPartitionFileVersionToMax(partitionId, newMaxVersion);
    }
  }

  public void removePartitions(
      List<ConsensusGroupId> dataRegionIdList, TimePartitionFilter filter) {
    for (ConsensusGroupId dataRegionId : dataRegionIdList) {
      dataRegionMap.get(dataRegionId).removePartitions(filter);
    }
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

  /** get all merge lock of the storage group processor related to the query */
  // TODO: mergelockV2
  //    public Pair<
  //            List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor,
  // List<PartialPath>>>
  //    mergeLock(List<PartialPath> pathList) throws StorageEngineException {
  //        Map<VirtualStorageGroupProcessor, List<PartialPath>> map = new HashMap<>();
  //        for (PartialPath path : pathList) {
  //            map.computeIfAbsent(getProcessor(path.getDevicePath()), key -> new
  // ArrayList<>()).add(path);
  //        }
  //        List<VirtualStorageGroupProcessor> list =
  //                map.keySet().stream()
  //
  // .sorted(Comparator.comparing(VirtualStorageGroupProcessor::getVirtualStorageGroupId))
  //                        .collect(Collectors.toList());
  //        list.forEach(VirtualStorageGroupProcessor::readLock);
  //
  //        return new Pair<>(list, map);
  //    }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<VirtualStorageGroupProcessor> list) {
    list.forEach(VirtualStorageGroupProcessor::readUnlock);
  }

  static class InstanceHolder {

    private static final StorageEngineV2 INSTANCE = new StorageEngineV2();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
