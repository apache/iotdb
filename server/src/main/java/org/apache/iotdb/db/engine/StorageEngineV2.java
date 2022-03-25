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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.HashVirtualPartitioner;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.StorageGroupManager;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.VirtualPartitioner;
import org.apache.iotdb.db.exception.*;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
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
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

  /** storage group name -> storage group processor */
  private final ConcurrentHashMap<PartialPath, List<VirtualStorageGroupProcessor>> processorMap =
      new ConcurrentHashMap<>();

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);

  private ScheduledExecutorService ttlCheckThread;
  private ScheduledExecutorService seqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService unseqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService tsFileTimedCloseCheckThread;

  /** virtual storage group partitioner */
  VirtualPartitioner partitioner = HashVirtualPartitioner.getInstance();

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

    // recover all logic storage group processors
    List<IStorageGroupMNode> sgNodes = IoTDB.schemaEngine.getAllStorageGroupNodes();
    List<Future<Void>> futures = new LinkedList<>();
    for (IStorageGroupMNode storageGroup : sgNodes) {
      StorageGroupManager storageGroupManager =
              processorMap.computeIfAbsent(
                      storageGroup.getPartialPath(), id -> new StorageGroupManager(true));

      // recover all virtual storage groups in one logic storage group
      storageGroupManager.asyncRecover(storageGroup, recoveryThreadPool, futures);
    }

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

  public void recoverV2() {
    setAllSgReady(false);
    recoveryThreadPool =
            IoTDBThreadPoolFactory.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");

    // recover all logic storage group processors
    List<IStorageGroupMNode> sgNodes = IoTDB.schemaEngine.getAllStorageGroupNodes();
    List<Future<Void>> futures = new LinkedList<>();
    for (IStorageGroupMNode storageGroup : sgNodes) {
      StorageGroupManager storageGroupManager =
              processorMap.computeIfAbsent(
                      storageGroup.getPartialPath(), id -> new ArrayList<>());

      // recover all virtual storage groups in one logic storage group
      storageGroupManager.asyncRecover(storageGroup, recoveryThreadPool, futures);
    }

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
      for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
        for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
          if (virtualStorageGroupProcessor != null) {
            virtualStorageGroupProcessor.checkFilesTTL();
          }
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
      for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
        for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
          if (virtualStorageGroupProcessor != null) {
            virtualStorageGroupProcessor.timedFlushSeqMemTable();
          }
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing sequence memtables", e);
    }
  }

  private void timedFlushUnseqMemTable() {
    try {
      for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
        for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
          if (virtualStorageGroupProcessor != null) {
            virtualStorageGroupProcessor.timedFlushUnseqMemTable();
          }
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed flushing unsequence memtables", e);
    }
  }

  private void timedCloseTsFileProcessor() {
    try {
      for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
        for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
          if (virtualStorageGroupProcessor != null) {
            virtualStorageGroupProcessor.timedCloseTsFileProcessor();
          }
        }
      }
    } catch (Exception e) {
      logger.error("An error occurred when timed closing tsfiles interval", e);
    }
  }

  @Override
  public void stop() {
    for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor vsg : DataRegionList) {
        if (vsg != null) {
          ThreadUtils.stopThreadPool(
              vsg.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
          ThreadUtils.stopThreadPool(vsg.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
        }
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
    for (PartialPath storageGroup : IoTDB.schemaEngine.getAllStorageGroupPaths()) {
      this.releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroup);
    }
    processorMap.clear();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
        for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
          ThreadUtils.stopThreadPool(
              virtualStorageGroupProcessor.getTimedCompactionScheduleTask(),
              ThreadName.COMPACTION_SCHEDULE);
          ThreadUtils.stopThreadPool(
              virtualStorageGroupProcessor.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
        }
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
    processorMap.clear();
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
   * This method is for sync, delete tsfile or sth like them, just get storage group directly by sg
   * name
   *
   * @param path storage group path
   * @return storage group processor
   */
  public VirtualStorageGroupProcessor getProcessorDirectly(PartialPath path)
      throws StorageEngineException {
    PartialPath storageGroupPath;
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaEngine.getStorageGroupNodeByPath(path);
      storageGroupPath = storageGroupMNode.getPartialPath();
      return getStorageGroupProcessorByPath(storageGroupPath, storageGroupMNode);
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * This method is for insert and query or sth like them, this may get a virtual storage group
   *
   * @param path device path
   * @return storage group processor
   */
  public VirtualStorageGroupProcessor getProcessor(PartialPath path) throws StorageEngineException {
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaEngine.getStorageGroupNodeByPath(path);
      return getStorageGroupProcessorByPath(path, storageGroupMNode);
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get lock holder for each sg
   *
   * @return storage group processor
   */
  public List<String> getLockInfo(List<PartialPath> pathList) throws StorageEngineException {
    try {
      List<String> lockHolderList = new ArrayList<>(pathList.size());
      for (PartialPath path : pathList) {
        IStorageGroupMNode storageGroupMNode = IoTDB.schemaEngine.getStorageGroupNodeByPath(path);
        VirtualStorageGroupProcessor virtualStorageGroupProcessor =
            getStorageGroupProcessorByPath(path, storageGroupMNode);
        lockHolderList.add(virtualStorageGroupProcessor.getInsertWriteLockHolder());
      }
      return lockHolderList;
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get storage group processor by device path
   *
   * @param devicePath path of the device
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  private VirtualStorageGroupProcessor getStorageGroupProcessorByPath(
      PartialPath devicePath, IStorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    List<VirtualStorageGroupProcessor> dataRegionList =
        processorMap.get(storageGroupMNode.getPartialPath());
    if (dataRegionList == null) {
      synchronized (this) {
        dataRegionList = processorMap.get(storageGroupMNode.getPartialPath());
        if (dataRegionList == null) {
          dataRegionList = new ArrayList<>();
          processorMap.put(storageGroupMNode.getPartialPath(), dataRegionList);
        }
      }
    }
    return getProcessor(devicePath, storageGroupMNode);
  }

  /**
   * get processor from device id
   *
   * @param partialPath device path
   * @return virtual storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  public VirtualStorageGroupProcessor getProcessor(
      PartialPath partialPath, IStorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    int loc = partitioner.deviceToVirtualStorageGroupId(partialPath);

    VirtualStorageGroupProcessor processor = virtualStorageGroupProcessor[loc];
    if (processor == null) {
      // if finish recover
      if (isVsgReady[loc].get()) {
        synchronized (storageGroupMNode) {
          processor = virtualStorageGroupProcessor[loc];
          if (processor == null) {
            processor =
                StorageEngine.getInstance()
                    .buildNewStorageGroupProcessor(
                        storageGroupMNode.getPartialPath(), storageGroupMNode, String.valueOf(loc));
            virtualStorageGroupProcessor[loc] = processor;
          }
        }
      } else {
        // not finished recover, refuse the request
        throw new StorageGroupNotReadyException(
            storageGroupMNode.getFullPath(), TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
      }
    }

    return processor;
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public VirtualStorageGroupProcessor buildNewStorageGroupProcessor(
      PartialPath logicalStorageGroupName,
      IStorageGroupMNode storageGroupMNode,
      String virtualStorageGroupId)
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
            storageGroupMNode.getFullPath());
    processor.setDataTTL(storageGroupMNode.getDataTTL());
    processor.setCustomFlushListeners(customFlushListeners);
    processor.setCustomCloseFileListeners(customCloseFileListeners);
    return processor;
  }

  /** This function is just for unit test. */
  @TestOnly
  public synchronized void reset() {
    for (List<VirtualStorageGroupProcessor> dataRegionList : processorMap.values()) {
      dataRegionList.clear();
    }
  }

  /**
   * insert an InsertRowPlan to a storage group.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws StorageEngineException, MetadataException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }

    VirtualStorageGroupProcessor virtualStorageGroupProcessor =
        getProcessor(insertRowPlan.getDevicePath());
    getSeriesSchemas(insertRowPlan, virtualStorageGroupProcessor);
    try {
      insertRowPlan.transferType();
    } catch (QueryProcessException e) {
      throw new StorageEngineException(e);
    }

    try {
      virtualStorageGroupProcessor.insert(insertRowPlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws StorageEngineException, MetadataException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }

    VirtualStorageGroupProcessor virtualStorageGroupProcessor =
        getProcessor(insertRowsOfOneDevicePlan.getDevicePath());

    for (InsertRowPlan plan : insertRowsOfOneDevicePlan.getRowPlans()) {
      plan.setMeasurementMNodes(new IMeasurementMNode[plan.getMeasurements().length]);
      // check whether types are match
      getSeriesSchemas(plan, virtualStorageGroupProcessor);
    }

    // TODO monitor: update statistics
    try {
      virtualStorageGroupProcessor.insert(insertRowsOfOneDevicePlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /** insert a InsertTabletPlan to a storage group */
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws StorageEngineException, BatchProcessException, MetadataException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }
    VirtualStorageGroupProcessor virtualStorageGroupProcessor;
    try {
      virtualStorageGroupProcessor = getProcessor(insertTabletPlan.getDevicePath());
    } catch (StorageEngineException e) {
      throw new StorageEngineException(
          String.format(
              "Get StorageGroupProcessor of device %s " + "failed",
              insertTabletPlan.getDevicePath()),
          e);
    }

    getSeriesSchemas(insertTabletPlan, virtualStorageGroupProcessor);
    virtualStorageGroupProcessor.insertTablet(insertTabletPlan);
  }

  /** insert an InsertTabletNode to a storage group */
  public void insertTabletV2(
      VirtualStorageGroupProcessor virtualStorageGroupProcessor, InsertTabletNode insertTabletNode)
      throws StorageEngineException, BatchProcessException, MetadataException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletNode.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }
    virtualStorageGroupProcessor.insertTablet(insertTabletNode);
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
        }
      }
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all storage group processor");
    for (List<VirtualStorageGroupProcessor> DataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : DataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
        }
      }
    }
  }

  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, boolean isSeq, boolean isSync) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    List<VirtualStorageGroupProcessor> virtualStorageGroupProcessorList =
        processorMap.get(storageGroupPath);
    for (VirtualStorageGroupProcessor processor : virtualStorageGroupProcessorList) {
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
   * @param storageGroupPath the storage group name
   * @param partitionId the partition id
   * @param isSeq is sequence tsfile or unsequence tsfile
   * @param isSync close tsfile synchronously or asynchronously
   * @throws StorageGroupNotSetException
   */
  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, long partitionId, boolean isSeq, boolean isSync)
      throws StorageGroupNotSetException {
    if (!processorMap.containsKey(storageGroupPath)) {
      throw new StorageGroupNotSetException(storageGroupPath.getFullPath());
    }

    List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
    for (VirtualStorageGroupProcessor processor : dataRegionList) {
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

  public void delete(
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.schemaEngine.getBelongedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        List<PartialPath> possiblePaths = path.alterPrefixPath(storageGroupPath);
        for (PartialPath possiblePath : possiblePaths) {
          List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
          for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
            if (virtualStorageGroupProcessor != null) {
              virtualStorageGroupProcessor.delete(
                  path, startTime, endTime, planIndex, timePartitionFilter);
            }
          }
        }
      }
    } catch (IOException | MetadataException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** delete data of timeseries "{deviceId}.{measurementId}" */
  public void deleteTimeseries(
      PartialPath path, long planIndex, TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.schemaEngine.getBelongedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        List<PartialPath> possiblePaths = path.alterPrefixPath(storageGroupPath);
        for (PartialPath possiblePath : possiblePaths) {
          List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
          for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
            if (virtualStorageGroupProcessor != null) {
              virtualStorageGroupProcessor.delete(
                  path, Long.MIN_VALUE, Long.MAX_VALUE, planIndex, timePartitionFilter);
            }
          }
        }
      }
    } catch (IOException | MetadataException e) {
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
    for (List<VirtualStorageGroupProcessor> dataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          totalUpgradeFileNum += virtualStorageGroupProcessor.countUpgradeFiles();
        }
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
    for (List<VirtualStorageGroupProcessor> dataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.upgrade();
        }
      }
    }
  }

  public void getResourcesToBeSettled(
      PartialPath storageGroupPath,
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths)
      throws StorageEngineException {
    if (processorMap.containsKey(storageGroupPath)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);

      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          if (!virtualStorageGroupProcessor.getIsSettling().compareAndSet(false, true)) {
            throw new StorageEngineException(
                    "Storage Group " + storageGroupPath.getFullPath() + " is already being settled now.");
          }
          virtualStorageGroupProcessor.addSettleFilesToList(
                  seqResourcesToBeSettled, unseqResourcesToBeSettled, tsFilePaths);
        }
      }
    } else {
      throw new StorageEngineException(
              "The Storage Group " + storageGroupPath.toString() + " is not existed.");
    }
  }

  public void setSettling(PartialPath storageGroupPath, boolean isSettling) {
    if (processorMap.containsKey(storageGroupPath)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.setSettling(isSettling);
        }
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
    for (List<VirtualStorageGroupProcessor> dataRegionList : processorMap.values()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.compact();
        }
      }
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      syncDeleteDataFiles(storageGroupPath);
    }
  }

  private void syncDeleteDataFiles(PartialPath storageGroupPath) {
    logger.info("Force to delete the data in storage group processor {}", storageGroupPath);
    if (processorMap.containsKey(storageGroupPath)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.syncDeleteDataFiles();
        }
      }
    }
  }

  /** release all the allocated non-heap */
  public void releaseWalDirectByteBufferPoolInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.releaseWalDirectByteBufferPool();
        }
      }
    }
  }

  /** delete all data of storage groups' timeseries. */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (PartialPath storageGroup : IoTDB.schemaEngine.getAllStorageGroupPaths()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    return true;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) {
    if (processorMap.containsKey(storageGroup)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroup);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.setDataTTL(dataTTL);
        }
      }
    }
  }

  public void deleteStorageGroup(PartialPath storageGroupPath) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }
    deleteAllDataFilesInOneStorageGroup(storageGroupPath);
    releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroupPath);
    List<VirtualStorageGroupProcessor> dataRegionList = processorMap.remove(storageGroupPath);
    for (VirtualStorageGroupProcessor dataRegion : dataRegionList) {
      dataRegion.deleteFolder(systemDir + File.pathSeparator + storageGroupPath);
      ThreadUtils.stopThreadPool(
          dataRegion.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
      ThreadUtils.stopThreadPool(dataRegion.getWALTrimScheduleTask(), ThreadName.WAL_TRIM);
    }
  }

  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws StorageEngineException, LoadFileException, IllegalPathException {
    getProcessorDirectly(new PartialPath(getSgByEngineFile(newTsFileResource.getTsFile(), false)))
        .loadNewTsFileForSync(newTsFileResource);
  }

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws LoadFileException, StorageEngineException, MetadataException {
    Set<String> deviceSet = newTsFileResource.getDevices();
    if (deviceSet == null || deviceSet.isEmpty()) {
      throw new StorageEngineException("Can not get the corresponding storage group.");
    }
    String device = deviceSet.iterator().next();
    PartialPath devicePath = new PartialPath(device);
    PartialPath storageGroupPath = IoTDB.schemaEngine.getBelongedStorageGroup(devicePath);
    getProcessorDirectly(storageGroupPath).loadNewTsFile(newTsFileResource);
  }

  public boolean deleteTsfileForSync(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile, false)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean deleteTsfile(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile, true)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean unloadTsfile(File tsfileToBeUnloaded, File targetDir)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(tsfileToBeUnloaded, true)))
        .unloadTsfile(tsfileToBeUnloaded, targetDir);
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

  /** @return TsFiles (seq or unseq) grouped by their storage group and partition number. */
  public Map<PartialPath, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<PartialPath, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<PartialPath, List<VirtualStorageGroupProcessor>> entry : processorMap.entrySet()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : entry.getValue()) {
        if (virtualStorageGroupProcessor != null) {
          List<TsFileResource> allResources = virtualStorageGroupProcessor.getSequenceFileTreeSet();
          allResources.addAll(virtualStorageGroupProcessor.getUnSequenceFileList());
          for (TsFileResource tsfile : allResources) {
            if (!tsfile.isClosed()) {
              continue;
            }
            long partitionNum = tsfile.getTimePartition();
            Map<Long, List<TsFileResource>> storageGroupFiles =
                ret.computeIfAbsent(entry.getKey(), n -> new HashMap<>());
            storageGroupFiles.computeIfAbsent(partitionNum, n -> new ArrayList<>()).add(tsfile);
          }
        }
      }
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(
      TsFileResource tsFileResource, PartialPath storageGroup, long partitionNum) {
    List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroup);
    if (dataRegionList == null) {
      return false;
    }

    Iterator<String> partialPathIterator = tsFileResource.getDevices().iterator();
    try {
      return getProcessor(new PartialPath(partialPathIterator.next()))
          .isFileAlreadyExist(tsFileResource, partitionNum);
    } catch (StorageEngineException | IllegalPathException e) {
      logger.error("can't find processor with: " + tsFileResource, e);
    }

    return false;
  }

  /**
   * Set the version of given partition to newMaxVersion if it is larger than the current version.
   */
  public void setPartitionVersionToMax(
      PartialPath storageGroup, long partitionId, long newMaxVersion) {
    if (processorMap.containsKey(storageGroup)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroup);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.setPartitionFileVersionToMax(partitionId, newMaxVersion);
        }
      }
    }
  }

  public void removePartitions(PartialPath storageGroupPath, TimePartitionFilter filter) {
    if (processorMap.containsKey(storageGroupPath)) {
      List<VirtualStorageGroupProcessor> dataRegionList = processorMap.get(storageGroupPath);
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : dataRegionList) {
        if (virtualStorageGroupProcessor != null) {
          virtualStorageGroupProcessor.removePartitions(filter);
        }
      }
    }
  }

  public Map<PartialPath, List<VirtualStorageGroupProcessor>> getProcessorMap() {
    return processorMap;
  }

  /**
   * Get a map indicating which storage groups have working TsFileProcessors and its associated
   * partitionId and whether it is sequence or not.
   *
   * @return storage group -> a list of partitionId-isSequence pairs
   */
  public Map<String, List<Pair<Long, Boolean>>> getWorkingStorageGroupPartitions() {
    Map<String, List<Pair<Long, Boolean>>> res = new ConcurrentHashMap<>();
    for (Entry<PartialPath, List<VirtualStorageGroupProcessor>> entry : processorMap.entrySet()) {
      for (VirtualStorageGroupProcessor virtualStorageGroupProcessor : entry.getValue()) {
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

          res.put(entry.getKey().getFullPath(), partitionIdList);
        }
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
  public Pair<
          List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
      mergeLock(List<PartialPath> pathList) throws StorageEngineException {
    Map<VirtualStorageGroupProcessor, List<PartialPath>> map = new HashMap<>();
    for (PartialPath path : pathList) {
      map.computeIfAbsent(getProcessor(path.getDevicePath()), key -> new ArrayList<>()).add(path);
    }
    List<VirtualStorageGroupProcessor> list =
        map.keySet().stream()
            .sorted(Comparator.comparing(VirtualStorageGroupProcessor::getVirtualStorageGroupId))
            .collect(Collectors.toList());
    list.forEach(VirtualStorageGroupProcessor::readLock);

    return new Pair<>(list, map);
  }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<VirtualStorageGroupProcessor> list) {
    list.forEach(VirtualStorageGroupProcessor::readUnlock);
  }

  /** @return virtual storage group name, like root.sg1/0 */
  public String getStorageGroupPath(PartialPath path) throws StorageEngineException {
    PartialPath deviceId = path.getDevicePath();
    VirtualStorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.getLogicalStorageGroupName()
        + File.separator
        + storageGroupProcessor.getVirtualStorageGroupId();
  }

  protected void getSeriesSchemas(InsertPlan insertPlan, VirtualStorageGroupProcessor processor)
      throws StorageEngineException, MetadataException {
    try {
      if (config.isEnableIDTable()) {
        processor.getIdTable().getSeriesSchemas(insertPlan);
      } else {
        IoTDB.schemaEngine.getSeriesSchemasAndReadLockDevice(insertPlan);
        insertPlan.setDeviceID(
            DeviceIDFactory.getInstance().getDeviceID(insertPlan.getDevicePath()));
      }
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }

  static class InstanceHolder {

    private static final StorageEngineV2 INSTANCE = new StorageEngineV2();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
