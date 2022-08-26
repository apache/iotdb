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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.DataRegion.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.dataregion.StorageGroupManager;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.idtable.deviceID.DeviceIDFactory;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.ThreadUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.wal.exception.WALException;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StorageEngine implements IService {
  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

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
  private final ConcurrentHashMap<PartialPath, StorageGroupManager> processorMap =
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

  private StorageEngine() {}

  public static StorageEngine getInstance() {
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
    StorageEngine.timePartitionInterval = timePartitionInterval;
  }

  public static long getTimePartition(long time) {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return enablePartition ? time / timePartitionInterval : 0;
  }

  public static boolean isEnablePartition() {
    return enablePartition;
  }

  @TestOnly
  public static void setEnablePartition(boolean enablePartition) {
    StorageEngine.enablePartition = enablePartition;
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
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.DATA_REGION_RECOVER_SERVICE.getName());

    List<IStorageGroupMNode> sgNodes = IoTDB.schemaProcessor.getAllStorageGroupNodes();
    // init wal recover manager
    WALRecoverManager.getInstance()
        .setAllDataRegionScannedLatch(
            new CountDownLatch(sgNodes.size() * config.getDataRegionNum()));
    // recover all logic storage groups
    List<Future<Void>> futures = new LinkedList<>();
    for (IStorageGroupMNode storageGroup : sgNodes) {
      StorageGroupManager storageGroupManager =
          processorMap.computeIfAbsent(
              storageGroup.getPartialPath(), id -> new StorageGroupManager(true));

      // recover all virtual storage groups in each logic storage group
      storageGroupManager.asyncRecover(storageGroup, recoveryThreadPool, futures);
    }

    // wait until wal is recovered
    try {
      WALRecoverManager.getInstance().recover();
    } catch (WALException e) {
      logger.error("Fail to recover wal.", e);
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
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        ttlCheckThread,
        this::checkTTL,
        TTL_CHECK_INTERVAL,
        TTL_CHECK_INTERVAL,
        TimeUnit.MILLISECONDS);
    logger.info("start ttl check thread successfully.");

    startTimedService();
  }

  private void checkTTL() {
    try {
      for (StorageGroupManager processor : processorMap.values()) {
        processor.checkTTL();
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
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          seqMemtableTimedFlushCheckThread,
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
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          unseqMemtableTimedFlushCheckThread,
          this::timedFlushUnseqMemTable,
          config.getUnseqMemtableFlushCheckInterval(),
          config.getUnseqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      logger.info("start unsequence memtable timed flush check thread successfully.");
    }
  }

  private void timedFlushSeqMemTable() {
    for (StorageGroupManager processor : processorMap.values()) {
      processor.timedFlushSeqMemTable();
    }
  }

  private void timedFlushUnseqMemTable() {
    for (StorageGroupManager processor : processorMap.values()) {
      processor.timedFlushUnseqMemTable();
    }
  }

  @Override
  public void stop() {
    for (StorageGroupManager storageGroupManager : processorMap.values()) {
      storageGroupManager.stopSchedulerPool();
    }
    syncCloseAllProcessor();
    ThreadUtils.stopThreadPool(ttlCheckThread, ThreadName.TTL_CHECK_SERVICE);
    ThreadUtils.stopThreadPool(
        seqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_SEQ_MEMTABLE);
    ThreadUtils.stopThreadPool(
        unseqMemtableTimedFlushCheckThread, ThreadName.TIMED_FlUSH_UNSEQ_MEMTABLE);
    recoveryThreadPool.shutdownNow();
    processorMap.clear();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      for (StorageGroupManager storageGroupManager : processorMap.values()) {
        storageGroupManager.stopSchedulerPool();
      }
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    shutdownTimedService(ttlCheckThread, "TTlCheckThread");
    shutdownTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    shutdownTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
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

  /** reboot timed flush sequence/unsequence memetable thread */
  public void rebootTimedService() throws ShutdownException {
    logger.info("Start rebooting all timed service.");

    // exclude ttl check thread
    stopTimedServiceAndThrow(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(
        unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");

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
  public DataRegion getProcessorDirectly(PartialPath path) throws StorageEngineException {
    PartialPath storageGroupPath;
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
      storageGroupPath = storageGroupMNode.getPartialPath();
      return getStorageGroupProcessorByPath(storageGroupPath, storageGroupMNode);
    } catch (DataRegionException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * This method is for sync, delete tsfile or sth like them, just get storage group directly by
   * dataRegionId
   *
   * @param path storage group path
   * @param dataRegionId dataRegionId
   * @return storage group processor
   */
  public DataRegion getProcessorDirectly(PartialPath path, int dataRegionId)
      throws StorageEngineException {
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
      return getStorageGroupProcessorById(dataRegionId, storageGroupMNode);
    } catch (DataRegionException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * This method is for insert and query or sth like them, this may get a virtual storage group
   *
   * @param path device path
   * @return storage group processor
   */
  public DataRegion getProcessor(PartialPath path) throws StorageEngineException {
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
      return getStorageGroupProcessorByPath(path, storageGroupMNode);
    } catch (DataRegionException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  public DataRegion getProcessorByDataRegionId(PartialPath path, int dataRegionId)
      throws StorageEngineException {
    try {
      IStorageGroupMNode storageGroupMNode = IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
      return getStorageGroupManager(storageGroupMNode)
          .getProcessor(storageGroupMNode, dataRegionId);
    } catch (DataRegionException | MetadataException e) {
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
        IStorageGroupMNode storageGroupMNode =
            IoTDB.schemaProcessor.getStorageGroupNodeByPath(path);
        DataRegion dataRegion = getStorageGroupProcessorByPath(path, storageGroupMNode);
        lockHolderList.add(dataRegion.getInsertWriteLockHolder());
      }
      return lockHolderList;
    } catch (DataRegionException | MetadataException e) {
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
  private DataRegion getStorageGroupProcessorByPath(
      PartialPath devicePath, IStorageGroupMNode storageGroupMNode)
      throws DataRegionException, StorageEngineException {
    return getStorageGroupManager(storageGroupMNode).getProcessor(devicePath, storageGroupMNode);
  }

  /**
   * get storage group processor by dataRegionId
   *
   * @param dataRegionId dataRegionId
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group processor
   */
  private DataRegion getStorageGroupProcessorById(
      int dataRegionId, IStorageGroupMNode storageGroupMNode)
      throws DataRegionException, StorageEngineException {
    return getStorageGroupManager(storageGroupMNode).getProcessor(dataRegionId, storageGroupMNode);
  }
  /**
   * get storage group manager by storage group mnode
   *
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group manager
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  private StorageGroupManager getStorageGroupManager(IStorageGroupMNode storageGroupMNode) {
    StorageGroupManager storageGroupManager = processorMap.get(storageGroupMNode.getPartialPath());
    if (storageGroupManager == null) {
      synchronized (this) {
        storageGroupManager = processorMap.get(storageGroupMNode.getPartialPath());
        if (storageGroupManager == null) {
          storageGroupManager = new StorageGroupManager();
          processorMap.put(storageGroupMNode.getPartialPath(), storageGroupManager);
        }
      }
    }
    return storageGroupManager;
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public DataRegion buildNewStorageGroupProcessor(
      PartialPath logicalStorageGroupName,
      IStorageGroupMNode storageGroupMNode,
      String virtualStorageGroupId)
      throws DataRegionException {
    DataRegion processor;
    logger.info(
        "construct a processor instance, the storage group is {}, Thread is {}",
        logicalStorageGroupName,
        Thread.currentThread().getId());
    processor =
        new DataRegion(
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
    for (StorageGroupManager storageGroupManager : processorMap.values()) {
      storageGroupManager.reset();
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

    DataRegion dataRegion = getProcessor(insertRowPlan.getDevicePath());
    getSeriesSchemas(insertRowPlan, dataRegion);
    try {
      insertRowPlan.transferType();
    } catch (QueryProcessException e) {
      throw new StorageEngineException(e);
    }

    try {
      dataRegion.insert(insertRowPlan);
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

    DataRegion dataRegion = getProcessor(insertRowsOfOneDevicePlan.getDevicePath());

    for (InsertRowPlan plan : insertRowsOfOneDevicePlan.getRowPlans()) {
      plan.setMeasurementMNodes(new IMeasurementMNode[plan.getMeasurements().length]);
      // check whether types are match
      getSeriesSchemas(plan, dataRegion);
    }

    // TODO monitor: update statistics
    try {
      dataRegion.insert(insertRowsOfOneDevicePlan);
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
    DataRegion dataRegion;
    try {
      dataRegion = getProcessor(insertTabletPlan.getDevicePath());
    } catch (StorageEngineException e) {
      throw new StorageEngineException(
          String.format(
              "Get StorageGroupProcessor of device %s " + "failed",
              insertTabletPlan.getDevicePath()),
          e);
    }

    getSeriesSchemas(insertTabletPlan, dataRegion);
    dataRegion.insertTablet(insertTabletPlan);
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (StorageGroupManager processor : processorMap.values()) {
      processor.syncCloseAllWorkingTsFileProcessors();
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all storage group processor");
    for (StorageGroupManager processor : processorMap.values()) {
      processor.forceCloseAllWorkingTsFileProcessors();
    }
  }

  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, boolean isSeq, boolean isSync) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    StorageGroupManager storageGroupManager = processorMap.get(storageGroupPath);
    storageGroupManager.closeStorageGroupProcessor(isSeq, isSync);
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

    StorageGroupManager storageGroupManager = processorMap.get(storageGroupPath);
    storageGroupManager.closeStorageGroupProcessor(partitionId, isSeq, isSync);
  }

  public void delete(
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter timePartitionFilter)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.schemaProcessor.getBelongedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        List<PartialPath> possiblePaths = path.alterPrefixPath(storageGroupPath);
        for (PartialPath possiblePath : possiblePaths) {
          processorMap
              .get(storageGroupPath)
              .delete(possiblePath, startTime, endTime, planIndex, timePartitionFilter);
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
      List<PartialPath> sgPaths = IoTDB.schemaProcessor.getBelongedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        List<PartialPath> possiblePaths = path.alterPrefixPath(storageGroupPath);
        for (PartialPath possiblePath : possiblePaths) {
          processorMap
              .get(storageGroupPath)
              .delete(possiblePath, Long.MIN_VALUE, Long.MAX_VALUE, planIndex, timePartitionFilter);
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
    for (StorageGroupManager storageGroupManager : processorMap.values()) {
      totalUpgradeFileNum += storageGroupManager.countUpgradeFiles();
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
    for (StorageGroupManager storageGroupManager : processorMap.values()) {
      storageGroupManager.upgradeAll();
    }
  }

  public void getResourcesToBeSettled(
      PartialPath sgPath,
      List<TsFileResource> seqResourcesToBeSettled,
      List<TsFileResource> unseqResourcesToBeSettled,
      List<String> tsFilePaths)
      throws StorageEngineException {
    StorageGroupManager storageGroupManager = processorMap.get(sgPath);
    if (storageGroupManager == null) {
      throw new StorageEngineException(
          "The Storage Group " + sgPath.toString() + " is not existed.");
    }
    if (!storageGroupManager.getIsSettling().compareAndSet(false, true)) {
      throw new StorageEngineException(
          "Storage Group " + sgPath.getFullPath() + " is already being settled now.");
    }
    storageGroupManager.getResourcesToBeSettled(
        seqResourcesToBeSettled, unseqResourcesToBeSettled, tsFilePaths);
  }

  public void setSettling(PartialPath sgPath, boolean isSettling) {
    if (processorMap.get(sgPath) == null) {
      return;
    }
    processorMap.get(sgPath).setSettling(isSettling);
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

    for (StorageGroupManager storageGroupManager : processorMap.values()) {
      storageGroupManager.mergeAll();
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
    processorMap.get(storageGroupPath).syncDeleteDataFiles();
  }

  /** delete all data of storage groups' timeseries. */
  @TestOnly
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (PartialPath storageGroup : IoTDB.schemaProcessor.getAllStorageGroupPaths()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    processorMap.clear();
    return true;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) {
    // storage group has no data
    if (!processorMap.containsKey(storageGroup)) {
      return;
    }

    processorMap.get(storageGroup).setTTL(dataTTL);
  }

  public void deleteStorageGroup(PartialPath storageGroupPath) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }
    abortCompactionTaskForStorageGroup(storageGroupPath);
    deleteAllDataFilesInOneStorageGroup(storageGroupPath);
    StorageGroupManager storageGroupManager = processorMap.remove(storageGroupPath);
    storageGroupManager.deleteStorageGroupSystemFolder(systemDir);
    storageGroupManager.stopSchedulerPool();
  }

  private void abortCompactionTaskForStorageGroup(PartialPath storageGroupPath) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    StorageGroupManager manager = processorMap.get(storageGroupPath);
    manager.setAllowCompaction(false);
    manager.abortCompaction();
  }

  public void loadNewTsFile(TsFileResource newTsFileResource, boolean deleteOriginFile)
      throws LoadFileException, StorageEngineException, MetadataException {
    Set<String> deviceSet = newTsFileResource.getDevices();
    if (deviceSet == null || deviceSet.isEmpty()) {
      throw new StorageEngineException("The TsFile is empty, cannot be loaded.");
    }
    String device = deviceSet.iterator().next();
    PartialPath devicePath = new PartialPath(device);
    PartialPath storageGroupPath = IoTDB.schemaProcessor.getBelongedStorageGroup(devicePath);
    getProcessorDirectly(storageGroupPath).loadNewTsFile(newTsFileResource, deleteOriginFile);
  }

  public boolean deleteTsfile(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(
            new PartialPath(getSgByEngineFile(deletedTsfile, true)),
            getDataRegionIdByEngineFile(deletedTsfile, true))
        .deleteTsfile(deletedTsfile);
  }

  public boolean unloadTsfile(File tsfileToBeUnloaded, File targetDir)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(
            new PartialPath(getSgByEngineFile(tsfileToBeUnloaded, true)),
            getDataRegionIdByEngineFile(tsfileToBeUnloaded, true))
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

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @param needCheck check if the tsfile is an internal TsFile. If you make sure it is inside, no
   *     need to check
   * @return dataRegionId
   * @throws IllegalPathException throw if tsfile is not an internal TsFile
   */
  public int getDataRegionIdByEngineFile(File file, boolean needCheck) throws IllegalPathException {
    if (needCheck) {
      File dataDir =
          file.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile();
      if (dataDir.exists()) {
        String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
        for (String dir : dataDirs) {
          try {
            if (Files.isSameFile(Paths.get(dir), dataDir.toPath())) {
              return Integer.parseInt(file.getParentFile().getParentFile().getName());
            }
          } catch (IOException e) {
            throw new IllegalPathException(file.getAbsolutePath(), e.getMessage());
          }
        }
      }
      throw new IllegalPathException(file.getAbsolutePath(), "it's not an internal tsfile.");
    } else {
      return Integer.parseInt(file.getParentFile().getParentFile().getName());
    }
  }

  /**
   * Get all the closed tsfiles of each storage group.
   *
   * @return TsFiles (seq or unseq) grouped by their storage group and partition number.
   */
  public Map<PartialPath, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<PartialPath, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<PartialPath, StorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getAllClosedStorageGroupTsFile(entry.getKey(), ret);
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(
      TsFileResource tsFileResource, PartialPath storageGroup, long partitionNum) {
    StorageGroupManager storageGroupManager = processorMap.get(storageGroup);
    if (storageGroupManager == null) {
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
    processorMap.get(storageGroup).setPartitionVersionToMax(partitionId, newMaxVersion);
  }

  public void removePartitions(PartialPath storageGroupPath, TimePartitionFilter filter) {
    if (processorMap.get(storageGroupPath) != null) {
      processorMap.get(storageGroupPath).removePartitions(filter);
    }
  }

  public Map<PartialPath, StorageGroupManager> getProcessorMap() {
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
    for (Entry<PartialPath, StorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getWorkingStorageGroupPartitions(entry.getKey().getFullPath(), res);
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
  public Pair<List<DataRegion>, Map<DataRegion, List<PartialPath>>> mergeLock(
      List<PartialPath> pathList) throws StorageEngineException {
    Map<DataRegion, List<PartialPath>> map = new HashMap<>();
    for (PartialPath path : pathList) {
      map.computeIfAbsent(getProcessor(path.getDevicePath()), key -> new ArrayList<>()).add(path);
    }
    List<DataRegion> list =
        map.keySet().stream()
            .sorted(Comparator.comparing(DataRegion::getDataRegionId))
            .collect(Collectors.toList());
    list.forEach(DataRegion::readLock);

    return new Pair<>(list, map);
  }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<DataRegion> list) {
    list.forEach(DataRegion::readUnlock);
  }

  /**
   * Get the virtual storage group name.
   *
   * @return virtual storage group name, like root.sg1/0
   */
  public String getStorageGroupPath(PartialPath path) throws StorageEngineException {
    PartialPath deviceId = path.getDevicePath();
    DataRegion storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.getStorageGroupName()
        + File.separator
        + storageGroupProcessor.getDataRegionId();
  }

  protected void getSeriesSchemas(InsertPlan insertPlan, DataRegion processor)
      throws StorageEngineException, MetadataException {
    try {
      if (config.isEnableIDTable()) {
        processor.getIdTable().getSeriesSchemas(insertPlan);
      } else {
        IoTDB.schemaProcessor.getSeriesSchemasAndReadLockDevice(insertPlan);
        insertPlan.setDeviceID(
            DeviceIDFactory.getInstance().getDeviceID(insertPlan.getDevicePath()));
      }
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
