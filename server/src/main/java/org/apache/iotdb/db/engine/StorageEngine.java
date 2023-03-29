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

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.visitor.DataExecutionVisitor;
import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.load.LoadTsFileManager;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.utils.ThreadUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.exception.WALException;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class StorageEngine implements IService {
  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;
  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();

  /** Time range for dividing database, the time unit is the same with IoTDB's TimestampPrecision */
  private static long timePartitionInterval = -1;

  /**
   * a folder (system/databases/ by default) that persist system info. Each database will have a
   * subfolder under the systemDir.
   */
  private final String systemDir =
      FilePathUtils.regularizePath(config.getSystemDir()) + "databases";

  /** DataRegionId -> DataRegion */
  private final ConcurrentHashMap<DataRegionId, DataRegion> dataRegionMap =
      new ConcurrentHashMap<>();

  /** DataRegionId -> DataRegion which is being deleted */
  private final ConcurrentHashMap<DataRegionId, DataRegion> deletingDataRegionMap =
      new ConcurrentHashMap<>();

  /** Database name -> ttl, for region recovery only */
  private final Map<String, Long> ttlMapForRecover = new ConcurrentHashMap<>();

  /** number of ready data region */
  private AtomicInteger readyDataRegionNum;

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);

  private ScheduledExecutorService ttlCheckThread;
  private ScheduledExecutorService seqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService unseqMemtableTimedFlushCheckThread;

  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();
  /** used to do short-lived asynchronous tasks */
  private ExecutorService cachedThreadPool;
  // add customized listeners here for flush and close events
  private List<CloseFileListener> customCloseFileListeners = new ArrayList<>();
  private List<FlushListener> customFlushListeners = new ArrayList<>();
  private int recoverDataRegionNum = 0;

  private LoadTsFileManager loadTsFileManager = new LoadTsFileManager();

  private StorageEngine() {}

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static void initTimePartition() {
    timePartitionInterval = IoTDBDescriptor.getInstance().getConfig().getTimePartitionInterval();
  }

  public static long getTimePartitionInterval() {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return timePartitionInterval;
  }

  public static long getTimePartition(long time) {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return time / timePartitionInterval;
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

  public void updateTTLInfo(byte[] allTTLInformation) {
    if (allTTLInformation == null) {
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(allTTLInformation);
    int mapSize = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < mapSize; i++) {
      ttlMapForRecover.put(
          Objects.requireNonNull(ReadWriteIOUtils.readString(buffer)),
          ReadWriteIOUtils.readLong(buffer));
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
    cachedThreadPool =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.STORAGE_ENGINE_CACHED_SERVICE.getName());

    List<Future<Void>> futures = new LinkedList<>();
    asyncRecover(futures);

    // wait until wal is recovered
    if (!config.isClusterMode()
        || !config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      try {
        WALRecoverManager.getInstance().recover();
      } catch (WALException e) {
        logger.error("Fail to recover wal.", e);
      }
    }

    // operations after all data regions are recovered
    Thread recoverEndTrigger =
        new Thread(
            () -> {
              checkResults(futures, "StorageEngine failed to recover.");
              setAllSgReady(true);
              ttlMapForRecover.clear();
            });
    recoverEndTrigger.start();
  }

  private void asyncRecover(List<Future<Void>> futures) {
    Map<String, List<DataRegionId>> localDataRegionInfo = getLocalDataRegionInfo();
    localDataRegionInfo.values().forEach(list -> recoverDataRegionNum += list.size());
    readyDataRegionNum = new AtomicInteger(0);
    // init wal recover manager
    WALRecoverManager.getInstance()
        .setAllDataRegionScannedLatch(new CountDownLatch(recoverDataRegionNum));
    for (Map.Entry<String, List<DataRegionId>> entry : localDataRegionInfo.entrySet()) {
      String sgName = entry.getKey();
      for (DataRegionId dataRegionId : entry.getValue()) {
        Callable<Void> recoverDataRegionTask =
            () -> {
              DataRegion dataRegion = null;
              try {
                dataRegion =
                    buildNewDataRegion(
                        sgName,
                        dataRegionId,
                        ttlMapForRecover.getOrDefault(sgName, Long.MAX_VALUE));
              } catch (DataRegionException e) {
                logger.error(
                    "Failed to recover data region {}[{}]", sgName, dataRegionId.getId(), e);
              }
              dataRegionMap.put(dataRegionId, dataRegion);
              logger.info(
                  "Data regions have been recovered {}/{}",
                  readyDataRegionNum.incrementAndGet(),
                  recoverDataRegionNum);
              return null;
            };
        futures.add(cachedThreadPool.submit(recoverDataRegionTask));
      }
    }
  }

  /** get StorageGroup -> DataRegionIdList map from data/system directory. */
  public Map<String, List<DataRegionId>> getLocalDataRegionInfo() {
    File system = SystemFileFactory.INSTANCE.getFile(systemDir);
    File[] sgDirs = system.listFiles();
    Map<String, List<DataRegionId>> localDataRegionInfo = new HashMap<>();
    if (sgDirs == null) {
      return localDataRegionInfo;
    }
    for (File sgDir : sgDirs) {
      if (!sgDir.isDirectory()) {
        continue;
      }
      String sgName = sgDir.getName();
      List<DataRegionId> dataRegionIdList = new ArrayList<>();
      for (File dataRegionDir : sgDir.listFiles()) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        dataRegionIdList.add(new DataRegionId(Integer.parseInt(dataRegionDir.getName())));
      }
      localDataRegionInfo.put(sgName, dataRegionIdList);
    }
    return localDataRegionInfo;
  }

  @Override
  public void start() {
    // build time Interval to divide time partition
    initTimePartition();
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
      for (DataRegion dataRegion : dataRegionMap.values()) {
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
              ThreadName.TIMED_FLUSH_SEQ_MEMTABLE.getName());
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
              ThreadName.TIMED_FLUSH_UNSEQ_MEMTABLE.getName());
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
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        dataRegion.timedFlushSeqMemTable();
      }
    }
  }

  private void timedFlushUnseqMemTable() {
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        dataRegion.timedFlushUnseqMemTable();
      }
    }
  }

  @Override
  public void stop() {
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        ThreadUtils.stopThreadPool(
            dataRegion.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
      }
    }
    syncCloseAllProcessor();
    ThreadUtils.stopThreadPool(ttlCheckThread, ThreadName.TTL_CHECK_SERVICE);
    ThreadUtils.stopThreadPool(
        seqMemtableTimedFlushCheckThread, ThreadName.TIMED_FLUSH_SEQ_MEMTABLE);
    ThreadUtils.stopThreadPool(
        unseqMemtableTimedFlushCheckThread, ThreadName.TIMED_FLUSH_UNSEQ_MEMTABLE);
    if (cachedThreadPool != null) {
      cachedThreadPool.shutdownNow();
    }
    dataRegionMap.clear();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      for (DataRegion dataRegion : dataRegionMap.values()) {
        ThreadUtils.stopThreadPool(
            dataRegion.getTimedCompactionScheduleTask(), ThreadName.COMPACTION_SCHEDULE);
      }
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    shutdownTimedService(ttlCheckThread, "TTlCheckThread");
    shutdownTimedService(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    shutdownTimedService(unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");
    cachedThreadPool.shutdownNow();
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

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  /**
   * build a new data region
   *
   * @param dataRegionId data region id e.g. 1
   * @param logicalStorageGroupName database name e.g. root.sg1
   */
  public DataRegion buildNewDataRegion(
      String logicalStorageGroupName, DataRegionId dataRegionId, long ttl)
      throws DataRegionException {
    DataRegion dataRegion;
    logger.info(
        "construct a data region instance, the database is {}, Thread is {}",
        logicalStorageGroupName,
        Thread.currentThread().getId());
    dataRegion =
        new DataRegion(
            systemDir + File.separator + logicalStorageGroupName,
            String.valueOf(dataRegionId.getId()),
            fileFlushPolicy,
            logicalStorageGroupName);
    WRITING_METRICS.createFlushingMemTableStatusMetrics(dataRegionId);
    WRITING_METRICS.createDataRegionMemoryCostMetrics(dataRegion);
    dataRegion.setDataTTLWithTimePrecisionCheck(ttl);
    dataRegion.setCustomFlushListeners(customFlushListeners);
    dataRegion.setCustomCloseFileListeners(customCloseFileListeners);
    return dataRegion;
  }

  /** Write data into DataRegion. For standalone mode only. */
  public TSStatus write(DataRegionId groupId, PlanNode planNode) {
    return planNode.accept(new DataExecutionVisitor(), dataRegionMap.get(groupId));
  }

  /** This function is just for unit test. */
  @TestOnly
  public synchronized void reset() {
    dataRegionMap.clear();
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all database processor");
    List<Future<Void>> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        tasks.add(
            cachedThreadPool.submit(
                () -> {
                  dataRegion.syncCloseAllWorkingTsFileProcessors();
                  return null;
                }));
      }
    }
    checkResults(tasks, "Failed to sync close processor.");
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all database processor");
    List<Future<Void>> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        tasks.add(
            cachedThreadPool.submit(
                () -> {
                  dataRegion.forceCloseAllWorkingTsFileProcessors();
                  return null;
                }));
      }
    }
    checkResults(tasks, "Failed to force close processor.");
  }

  public void closeStorageGroupProcessor(String storageGroupPath, boolean isSeq) {
    List<Future<Void>> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion.getDatabaseName().equals(storageGroupPath)) {
        if (isSeq) {
          for (TsFileProcessor tsFileProcessor : dataRegion.getWorkSequenceTsFileProcessors()) {
            tasks.add(
                cachedThreadPool.submit(
                    () -> {
                      dataRegion.syncCloseOneTsFileProcessor(isSeq, tsFileProcessor);
                      return null;
                    }));
          }
        } else {
          for (TsFileProcessor tsFileProcessor : dataRegion.getWorkUnsequenceTsFileProcessors()) {
            tasks.add(
                cachedThreadPool.submit(
                    () -> {
                      dataRegion.syncCloseOneTsFileProcessor(isSeq, tsFileProcessor);
                      return null;
                    }));
          }
        }
      }
    }
    checkResults(tasks, "Failed to close database processor.");
  }

  private <V> void checkResults(List<Future<V>> tasks, String errorMsg) {
    for (Future<V> task : tasks) {
      try {
        task.get();
      } catch (ExecutionException e) {
        throw new StorageEngineFailureException(errorMsg, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(errorMsg, e);
      }
    }
  }

  /**
   * merge all databases.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll() throws StorageEngineException {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    dataRegionMap.values().forEach(DataRegion::compact);
  }

  public void operateFlush(TFlushReq req) {
    if (req.storageGroups == null) {
      StorageEngine.getInstance().syncCloseAllProcessor();
      WALManager.getInstance().deleteOutdatedWALFiles();
    } else {
      for (String storageGroup : req.storageGroups) {
        if (req.isSeq == null) {
          StorageEngine.getInstance().closeStorageGroupProcessor(storageGroup, true);
          StorageEngine.getInstance().closeStorageGroupProcessor(storageGroup, false);
        } else {
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(storageGroup, Boolean.parseBoolean(req.isSeq));
        }
      }
    }
  }

  public void clearCache() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  public void setTTL(List<DataRegionId> dataRegionIdList, long dataTTL) {
    for (DataRegionId dataRegionId : dataRegionIdList) {
      DataRegion dataRegion = dataRegionMap.get(dataRegionId);
      if (dataRegion != null) {
        dataRegion.setDataTTLWithTimePrecisionCheck(dataTTL);
      }
    }
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

  private void makeSureNoOldRegion(DataRegionId regionId) {
    while (deletingDataRegionMap.containsKey(regionId)) {
      DataRegion oldRegion = deletingDataRegionMap.get(regionId);
      if (oldRegion != null) {
        oldRegion.waitForDeleted();
      }
    }
  }

  // When registering a new region, the coordinator needs to register the corresponding region with
  // the local engine before adding the corresponding consensusGroup to the consensus layer
  public DataRegion createDataRegion(DataRegionId regionId, String sg, long ttl)
      throws DataRegionException {
    makeSureNoOldRegion(regionId);
    AtomicReference<DataRegionException> exceptionAtomicReference = new AtomicReference<>(null);
    DataRegion dataRegion =
        dataRegionMap.computeIfAbsent(
            regionId,
            x -> {
              try {
                return buildNewDataRegion(sg, x, ttl);
              } catch (DataRegionException e) {
                exceptionAtomicReference.set(e);
              }
              return null;
            });
    if (exceptionAtomicReference.get() != null) {
      throw exceptionAtomicReference.get();
    }
    return dataRegion;
  }

  public void deleteDataRegion(DataRegionId regionId) {
    if (!dataRegionMap.containsKey(regionId) || deletingDataRegionMap.containsKey(regionId)) {
      return;
    }
    DataRegion region =
        deletingDataRegionMap.computeIfAbsent(regionId, k -> dataRegionMap.remove(regionId));
    if (region != null) {
      region.markDeleted();
      WRITING_METRICS.removeDataRegionMemoryCostMetrics(regionId);
      WRITING_METRICS.removeFlushingMemTableStatusMetrics(regionId);
      try {
        region.abortCompaction();
        region.syncDeleteDataFiles();
        region.deleteFolder(systemDir);
        if (config.isClusterMode()
            && config
                .getDataRegionConsensusProtocolClass()
                .equals(ConsensusFactory.IOT_CONSENSUS)) {
          // delete wal
          WALManager.getInstance()
              .deleteWALNode(
                  region.getDatabaseName() + FILE_NAME_SEPARATOR + region.getDataRegionId());
          // delete snapshot
          for (String dataDir : config.getDataDirs()) {
            File regionSnapshotDir =
                new File(
                    dataDir + File.separator + IoTDBConstant.SNAPSHOT_FOLDER_NAME,
                    region.getDatabaseName() + FILE_NAME_SEPARATOR + regionId.getId());
            if (regionSnapshotDir.exists()) {
              try {
                FileUtils.deleteDirectory(regionSnapshotDir);
              } catch (IOException e) {
                logger.error("Failed to delete snapshot dir {}", regionSnapshotDir, e);
              }
            }
          }
        }
        SyncService.getInstance().unregisterDataRegion(region.getDataRegionId());
      } catch (Exception e) {
        logger.error(
            "Error occurs when deleting data region {}-{}",
            region.getDatabaseName(),
            region.getDataRegionId(),
            e);
      } finally {
        deletingDataRegionMap.remove(regionId);
      }
    }
  }

  public DataRegion getDataRegion(DataRegionId regionId) {
    return dataRegionMap.get(regionId);
  }

  public List<DataRegion> getAllDataRegions() {
    return new ArrayList<>(dataRegionMap.values());
  }

  public List<DataRegionId> getAllDataRegionIds() {
    return new ArrayList<>(dataRegionMap.keySet());
  }

  /** This method is not thread-safe */
  public void setDataRegion(DataRegionId regionId, DataRegion newRegion) {
    if (dataRegionMap.containsKey(regionId)) {
      DataRegion oldRegion = dataRegionMap.get(regionId);
      oldRegion.syncCloseAllWorkingTsFileProcessors();
      oldRegion.abortCompaction();
    }
    dataRegionMap.put(regionId, newRegion);
  }

  public TSStatus setTTL(TSetTTLReq req) {
    Map<String, List<DataRegionId>> localDataRegionInfo =
        StorageEngine.getInstance().getLocalDataRegionInfo();
    List<DataRegionId> dataRegionIdList = new ArrayList<>();
    req.storageGroupPathPattern.forEach(
        storageGroup -> dataRegionIdList.addAll(localDataRegionInfo.get(storageGroup)));
    for (DataRegionId dataRegionId : dataRegionIdList) {
      DataRegion dataRegion = dataRegionMap.get(dataRegionId);
      if (dataRegion != null) {
        dataRegion.setDataTTLWithTimePrecisionCheck(req.TTL);
      }
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public TsFileFlushPolicy getFileFlushPolicy() {
    return fileFlushPolicy;
  }

  public TSStatus writeLoadTsFileNode(
      DataRegionId dataRegionId, LoadTsFilePieceNode pieceNode, String uuid) {
    TSStatus status = new TSStatus();

    try {
      loadTsFileManager.writeToDataRegion(getDataRegion(dataRegionId), pieceNode, uuid);
    } catch (PageException e) {
      logger.error(
          String.format(
              "Parse Page error when writing piece node of TsFile %s to DataRegion %s.",
              pieceNode.getTsFile(), dataRegionId),
          e);
      status.setCode(TSStatusCode.LOAD_PIECE_OF_TSFILE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    } catch (IOException e) {
      logger.error(
          String.format(
              "IO error when writing piece node of TsFile %s to DataRegion %s.",
              pieceNode.getTsFile(), dataRegionId),
          e);
      status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    }

    return RpcUtils.SUCCESS_STATUS;
  }

  public TSStatus executeLoadCommand(LoadTsFileScheduler.LoadCommand loadCommand, String uuid) {
    TSStatus status = new TSStatus();

    try {
      switch (loadCommand) {
        case EXECUTE:
          if (loadTsFileManager.loadAll(uuid)) {
            status = RpcUtils.SUCCESS_STATUS;
          } else {
            status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
            status.setMessage(
                String.format(
                    "No load TsFile uuid %s recorded for execute load command %s.",
                    uuid, loadCommand));
          }
          break;
        case ROLLBACK:
          if (loadTsFileManager.deleteAll(uuid)) {
            status = RpcUtils.SUCCESS_STATUS;
          } else {
            status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
            status.setMessage(
                String.format(
                    "No load TsFile uuid %s recorded for execute load command %s.",
                    uuid, loadCommand));
          }
          break;
        default:
          status.setCode(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
          status.setMessage(String.format("Wrong load command %s.", loadCommand));
      }
    } catch (IOException | LoadFileException e) {
      logger.error(String.format("Execute load command %s error.", loadCommand), e);
      status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }

    return status;
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

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
