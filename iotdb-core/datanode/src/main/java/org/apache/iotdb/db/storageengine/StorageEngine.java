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
package org.apache.iotdb.db.storageengine;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.schema.ttl.TTLCache;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.UnsortedFileRepairTaskScheduler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.CloseFileListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALException;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;
import org.apache.iotdb.db.storageengine.load.limiter.LoadTsFileRateLimiter;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.ThreadUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class StorageEngine implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageEngine.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final WritingMetrics WRITING_METRICS = WritingMetrics.getInstance();

  /**
   * a folder (system/databases/ by default) that persist system info. Each database will have a
   * subfolder under the systemDir.
   */
  private static final String systemDir =
      FilePathUtils.regularizePath(CONFIG.getSystemDir()) + "databases";

  /** DataRegionId -> DataRegion */
  private final ConcurrentHashMap<DataRegionId, DataRegion> dataRegionMap =
      new ConcurrentHashMap<>();

  /** DataRegionId -> DataRegion which is being deleted */
  private final ConcurrentHashMap<DataRegionId, DataRegion> deletingDataRegionMap =
      new ConcurrentHashMap<>();

  /** number of ready data region */
  private AtomicInteger readyDataRegionNum;

  private final AtomicBoolean isReadyForReadAndWrite = new AtomicBoolean();

  private final AtomicBoolean isReadyForNonReadWriteFunctions = new AtomicBoolean();

  private ScheduledExecutorService seqMemtableTimedFlushCheckThread;
  private ScheduledExecutorService unseqMemtableTimedFlushCheckThread;

  private final TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();

  /** used to do short-lived asynchronous tasks */
  private ExecutorService cachedThreadPool;

  // add customized listeners here for flush and close events
  private final List<CloseFileListener> customCloseFileListeners = new ArrayList<>();
  private final List<FlushListener> customFlushListeners = new ArrayList<>();
  private int recoverDataRegionNum = 0;

  private final LoadTsFileManager loadTsFileManager = new LoadTsFileManager();

  private StorageEngine() {}

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static void initTimePartition() {
    TimePartitionUtils.setTimePartitionInterval(
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval());
  }

  /** block insertion if the insertion is rejected by memory control */
  public static void blockInsertionIfReject() throws WriteProcessRejectException {
    long startTime = System.currentTimeMillis();
    while (SystemInfo.getInstance().isRejected()) {
      try {
        TimeUnit.MILLISECONDS.sleep(CONFIG.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startTime > CONFIG.getMaxWaitingTimeWhenInsertBlocked()) {
          throw new WriteProcessRejectException(
              "System rejected over " + (System.currentTimeMillis() - startTime) + "ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isReadyForReadAndWrite() {
    return isReadyForReadAndWrite.get();
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  public boolean isReadyForNonReadWriteFunctions() {
    return isReadyForNonReadWriteFunctions.get();
  }

  private void asyncRecoverDataRegion() throws StartupException {
    long startRecoverTime = System.currentTimeMillis();
    isReadyForNonReadWriteFunctions.set(false);
    isReadyForReadAndWrite.set(false);
    cachedThreadPool =
        IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.STORAGE_ENGINE_CACHED_POOL.getName());

    List<Future<Void>> futures = new LinkedList<>();
    asyncRecover(futures);

    // wait until wal is recovered
    if (!CONFIG.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      try {
        WALRecoverManager.getInstance().recover();
      } catch (WALException e) {
        LOGGER.error("Fail to recover wal.", e);
      }
    }

    // operations after all data regions are recovered
    Thread recoverEndTrigger =
        new Thread(
            () -> {
              checkResults(futures, "StorageEngine failed to recover.");
              isReadyForReadAndWrite.set(true);
              LOGGER.info(
                  "Storage Engine recover cost: {}s.",
                  (System.currentTimeMillis() - startRecoverTime) / 1000);
            },
            ThreadName.STORAGE_ENGINE_RECOVER_TRIGGER.getName());
    recoverEndTrigger.start();
  }

  private void asyncRecover(List<Future<Void>> futures) {
    Map<String, List<DataRegionId>> localDataRegionInfo = getLocalDataRegionInfo();
    localDataRegionInfo.values().forEach(list -> recoverDataRegionNum += list.size());
    readyDataRegionNum = new AtomicInteger(0);
    // init wal recover manager
    WALRecoverManager.getInstance()
        .setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(recoverDataRegionNum));
    for (Map.Entry<String, List<DataRegionId>> entry : localDataRegionInfo.entrySet()) {
      String sgName = entry.getKey();
      for (DataRegionId dataRegionId : entry.getValue()) {
        Callable<Void> recoverDataRegionTask =
            () -> {
              DataRegion dataRegion;
              try {
                dataRegion = buildNewDataRegion(sgName, dataRegionId);
              } catch (DataRegionException e) {
                LOGGER.error(
                    "Failed to recover data region {}[{}]", sgName, dataRegionId.getId(), e);
                return null;
              }
              dataRegionMap.put(dataRegionId, dataRegion);
              LOGGER.info(
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
      for (File dataRegionDir : Objects.requireNonNull(sgDir.listFiles())) {
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
  public void start() throws StartupException {
    recoverDataRegionNum = 0;
    // build time Interval to divide time partition
    initTimePartition();
    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }

    asyncRecoverDataRegion();

    startTimedService();

    // wait here for dataRegionMap recovered
    while (!isReadyForReadAndWrite.get()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.warn("Storage engine failed to set up.", e);
        Thread.currentThread().interrupt();
        return;
      }
    }

    asyncRecoverTsFileResource();
  }

  private void startTimedService() {
    // timed flush sequence memtable
    if (CONFIG.isEnableTimedFlushSeqMemtable()) {
      seqMemtableTimedFlushCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.TIMED_FLUSH_SEQ_MEMTABLE.getName());
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          seqMemtableTimedFlushCheckThread,
          this::timedFlushSeqMemTable,
          CONFIG.getSeqMemtableFlushCheckInterval(),
          CONFIG.getSeqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      LOGGER.info("start sequence memtable timed flush check thread successfully.");
    }
    // timed flush unsequence memtable
    if (CONFIG.isEnableTimedFlushUnseqMemtable()) {
      unseqMemtableTimedFlushCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.TIMED_FLUSH_UNSEQ_MEMTABLE.getName());
      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          unseqMemtableTimedFlushCheckThread,
          this::timedFlushUnseqMemTable,
          CONFIG.getUnseqMemtableFlushCheckInterval(),
          CONFIG.getUnseqMemtableFlushCheckInterval(),
          TimeUnit.MILLISECONDS);
      LOGGER.info("start unsequence memtable timed flush check thread successfully.");
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

  private void asyncRecoverTsFileResource() {
    List<Future<Void>> futures = new LinkedList<>();
    long startRecoverTime = System.currentTimeMillis();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        List<Callable<Void>> asyncTsFileResourceRecoverTasks =
            dataRegion.getAsyncTsFileResourceRecoverTaskList();
        if (asyncTsFileResourceRecoverTasks != null) {
          Callable<Void> taskOfRegion =
              () -> {
                for (Callable<Void> task : asyncTsFileResourceRecoverTasks) {
                  task.call();
                }
                dataRegion.clearAsyncTsFileResourceRecoverTaskList();
                dataRegion.initCompactionSchedule();
                return null;
              };
          futures.add(cachedThreadPool.submit(taskOfRegion));
        }
      }
    }
    Thread recoverEndTrigger =
        new Thread(
            () -> {
              checkResults(futures, "async recover tsfile resource meets error.");
              recoverRepairData();
              isReadyForNonReadWriteFunctions.set(true);
              LOGGER.info(
                  "TsFile Resource recover cost: {}s.",
                  (System.currentTimeMillis() - startRecoverTime) / 1000);
            },
            ThreadName.STORAGE_ENGINE_RECOVER_TRIGGER.getName());
    recoverEndTrigger.start();
  }

  @Override
  public void stop() {
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null) {
        CompactionScheduleTaskManager.getInstance().unregisterDataRegion(dataRegion);
      }
    }
    syncCloseAllProcessor();
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
        if (dataRegion != null) {
          CompactionScheduleTaskManager.getInstance().unregisterDataRegion(dataRegion);
        }
      }
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
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
        LOGGER.warn("{} still doesn't exit after 30s", poolName);
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
   * @param databaseName database name e.g. root.sg1
   */
  public DataRegion buildNewDataRegion(String databaseName, DataRegionId dataRegionId)
      throws DataRegionException {
    DataRegion dataRegion;
    LOGGER.info(
        "construct a data region instance, the database is {}, Thread is {}",
        databaseName,
        Thread.currentThread().getId());
    dataRegion =
        new DataRegion(
            systemDir + File.separator + databaseName,
            String.valueOf(dataRegionId.getId()),
            fileFlushPolicy,
            databaseName);
    WRITING_METRICS.createFlushingMemTableStatusMetrics(dataRegionId);
    WRITING_METRICS.createDataRegionMemoryCostMetrics(dataRegion);
    WRITING_METRICS.createActiveMemtableCounterMetrics(dataRegionId);
    dataRegion.setCustomFlushListeners(customFlushListeners);
    dataRegion.setCustomCloseFileListeners(customCloseFileListeners);
    return dataRegion;
  }

  /** This function is just for unit test. */
  @TestOnly
  public synchronized void reset() {
    dataRegionMap.clear();
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    LOGGER.info("Start closing all database processor");
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
    LOGGER.info("Start force closing all database processor");
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

  public void syncCloseProcessorsInDatabase(String databaseName) {
    List<Future<Void>> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion != null && dataRegion.getDatabaseName().equals(databaseName)) {
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

  public void syncCloseProcessorsInDatabase(String databaseName, boolean isSeq) {
    List<Future<Void>> tasks = new ArrayList<>();
    for (DataRegion dataRegion : dataRegionMap.values()) {
      if (dataRegion.getDatabaseName().equals(databaseName)) {
        tasks.add(
            cachedThreadPool.submit(
                () -> {
                  dataRegion.syncCloseWorkingTsFileProcessors(isSeq);
                  return null;
                }));
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

  /**
   * check and repair unsorted data by compaction.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public boolean repairData() throws StorageEngineException {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    if (!CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart()) {
      return false;
    }
    LOGGER.info("start repair data");
    List<DataRegion> dataRegionList = new ArrayList<>(dataRegionMap.values());
    cachedThreadPool.submit(new UnsortedFileRepairTaskScheduler(dataRegionList, false));
    return true;
  }

  /**
   * stop repair data by interrupt
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void stopRepairData() throws StorageEngineException {
    CompactionScheduleTaskManager.RepairDataTaskManager repairDataTaskManager =
        CompactionScheduleTaskManager.getRepairTaskManagerInstance();
    if (!CompactionScheduleTaskManager.getRepairTaskManagerInstance().hasRunningRepairTask()) {
      return;
    }
    LOGGER.info("stop repair data");
    try {
      repairDataTaskManager.markRepairTaskStopping();
      repairDataTaskManager.abortRepairTask();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException ignored) {
    }
  }

  /** recover the progress of unfinished repair schedule task */
  public void recoverRepairData() {
    List<DataRegion> dataRegionList = new ArrayList<>(dataRegionMap.values());
    String repairLogDirPath =
        IoTDBDescriptor.getInstance().getConfig().getSystemDir()
            + File.separator
            + RepairLogger.repairLogDir;
    File repairLogDir = new File(repairLogDirPath);
    if (!repairLogDir.exists() || !repairLogDir.isDirectory()) {
      return;
    }
    File[] files = repairLogDir.listFiles();
    List<File> fileList =
        Stream.of(files == null ? new File[0] : files)
            .filter(
                f -> {
                  String fileName = f.getName();
                  return f.isFile()
                      && (RepairLogger.repairProgressFileName.equals(fileName)
                          || RepairLogger.repairProgressStoppedFileName.equals(fileName));
                })
            .collect(Collectors.toList());
    if (!fileList.isEmpty()) {
      CompactionScheduleTaskManager.getRepairTaskManagerInstance().markRepairTaskStart();
      cachedThreadPool.submit(new UnsortedFileRepairTaskScheduler(dataRegionList, true));
    }
  }

  public void operateFlush(TFlushReq req) {
    if (req.storageGroups == null || req.storageGroups.isEmpty()) {
      StorageEngine.getInstance().syncCloseAllProcessor();
      WALManager.getInstance().syncDeleteOutdatedFilesInWALNodes();
    } else {
      for (String databaseName : req.storageGroups) {
        if (req.isSeq == null) {
          StorageEngine.getInstance().syncCloseProcessorsInDatabase(databaseName);
        } else {
          StorageEngine.getInstance()
              .syncCloseProcessorsInDatabase(databaseName, Boolean.parseBoolean(req.isSeq));
        }
      }
    }
  }

  public void clearCache() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }

  public TSStatus setConfiguration(TSetConfigurationReq req) {
    TSStatus tsStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    Map<String, String> newConfigItems = req.getConfigs();
    if (newConfigItems.isEmpty()) {
      return tsStatus;
    }
    Properties properties = new Properties();
    properties.putAll(newConfigItems);

    URL configFileUrl = IoTDBDescriptor.getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
    if (configFileUrl == null || !(new File(configFileUrl.getFile()).exists())) {
      // configuration file not exist, update in mem
      try {
        IoTDBDescriptor.getInstance().loadHotModifiedProps(properties);
        IoTDBDescriptor.getInstance().reloadMetricProperties(properties);
      } catch (Exception e) {
        return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
      }
      return tsStatus;
    }

    // 1. append new configuration properties to configuration file
    try {
      ConfigurationFileUtils.updateConfigurationFile(new File(configFileUrl.getFile()), properties);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }

    // 2. load hot modified properties
    try {
      IoTDBDescriptor.getInstance().loadHotModifiedProps();
    } catch (Exception e) {
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return tsStatus;
  }

  /**
   * Add a listener to listen flush start/end events. Notice that this addition only applies to
   * TsFileProcessors created afterward.
   */
  public void registerFlushListener(FlushListener listener) {
    customFlushListeners.add(listener);
  }

  /**
   * Add a listener to listen file close events. Notice that this addition only applies to
   * TsFileProcessors created afterward.
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
  // the local storage before adding the corresponding consensusGroup to the consensus layer
  public void createDataRegion(DataRegionId regionId, String databaseName)
      throws DataRegionException {
    makeSureNoOldRegion(regionId);
    AtomicReference<DataRegionException> exceptionAtomicReference = new AtomicReference<>(null);
    dataRegionMap.computeIfAbsent(
        regionId,
        region -> {
          try {
            return buildNewDataRegion(databaseName, region);
          } catch (DataRegionException e) {
            exceptionAtomicReference.set(e);
          }
          return null;
        });

    if (exceptionAtomicReference.get() != null) {
      throw exceptionAtomicReference.get();
    }
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
      WRITING_METRICS.removeActiveMemtableCounterMetrics(regionId);
      try {
        region.abortCompaction();
        region.syncDeleteDataFiles();
        region.deleteFolder(systemDir);
        switch (CONFIG.getDataRegionConsensusProtocolClass()) {
          case ConsensusFactory.IOT_CONSENSUS:
          case ConsensusFactory.IOT_CONSENSUS_V2:
            // delete wal
            WALManager.getInstance()
                .deleteWALNode(
                    region.getDatabaseName() + FILE_NAME_SEPARATOR + region.getDataRegionId());
            // delete snapshot
            for (String dataDir : CONFIG.getLocalDataDirs()) {
              File regionSnapshotDir =
                  new File(
                      dataDir + File.separator + IoTDBConstant.SNAPSHOT_FOLDER_NAME,
                      region.getDatabaseName() + FILE_NAME_SEPARATOR + regionId.getId());
              if (regionSnapshotDir.exists()) {
                try {
                  FileUtils.deleteDirectory(regionSnapshotDir);
                } catch (IOException e) {
                  LOGGER.error("Failed to delete snapshot dir {}", regionSnapshotDir, e);
                }
              }
            }
            break;
          case ConsensusFactory.SIMPLE_CONSENSUS:
            // delete region information in wal and may delete wal
            WALManager.getInstance()
                .deleteRegionAndMayDeleteWALNode(
                    region.getDatabaseName(), region.getDataRegionId());
            break;
          case ConsensusFactory.RATIS_CONSENSUS:
          default:
            break;
        }
      } catch (Exception e) {
        LOGGER.error(
            "Error occurs when deleting data region {}-{}",
            region.getDatabaseName(),
            region.getDataRegionId(),
            e);
      } finally {
        deletingDataRegionMap.remove(regionId);
      }
    }
  }

  /**
   * run the runnable if the region is absent. if the region is present, do nothing.
   *
   * <p>we don't use computeIfAbsent because we don't want to create a new region if the region is
   * absent, we just want to run the runnable in a synchronized way.
   *
   * @return true if the region is absent and the runnable is run. false if the region is present.
   */
  public boolean runIfAbsent(DataRegionId regionId, Runnable runnable) {
    final AtomicBoolean result = new AtomicBoolean(false);
    dataRegionMap.computeIfAbsent(
        regionId,
        k -> {
          runnable.run();
          result.set(true);
          return null;
        });
    return result.get();
  }

  /**
   * run the consumer if the region is present. if the region is absent, do nothing.
   *
   * <p>we don't use computeIfPresent because we don't want to remove the region if the consumer
   * returns null, we just want to run the consumer in a synchronized way.
   *
   * @return true if the region is present and the consumer is run. false if the region is absent.
   */
  public boolean runIfPresent(DataRegionId regionId, Consumer<DataRegion> consumer) {
    final AtomicBoolean result = new AtomicBoolean(false);
    dataRegionMap.computeIfPresent(
        regionId,
        (id, region) -> {
          consumer.accept(region);
          result.set(true);
          return region;
        });
    return result.get();
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
      oldRegion.markDeleted();
      oldRegion.abortCompaction();
      oldRegion.syncCloseAllWorkingTsFileProcessors();
    }
    dataRegionMap.put(regionId, newRegion);
  }

  /** Update ttl cache in dataNode. */
  public TSStatus setTTL(TSetTTLReq req) throws IllegalPathException {
    String[] path = PathUtils.splitPathToDetachedNodes(req.getPathPattern().get(0));
    long ttl = req.getTTL();
    boolean isDataBase = req.isDataBase;
    if (ttl == TTLCache.NULL_TTL) {
      DataNodeTTLCache.getInstance().unsetTTLForTree(path);
      if (isDataBase) {
        // unset ttl to path.**
        String[] pathWithWildcard = Arrays.copyOf(path, path.length + 1);
        pathWithWildcard[pathWithWildcard.length - 1] = IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
        DataNodeTTLCache.getInstance().unsetTTLForTree(pathWithWildcard);
      }
    } else {
      DataNodeTTLCache.getInstance().setTTLForTree(path, ttl);
      if (isDataBase) {
        // set ttl to path.**
        String[] pathWithWildcard = Arrays.copyOf(path, path.length + 1);
        pathWithWildcard[pathWithWildcard.length - 1] = IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
        DataNodeTTLCache.getInstance().setTTLForTree(pathWithWildcard, ttl);
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

    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      status.setCode(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
      status.setMessage(LoadReadOnlyException.MESSAGE);
      return status;
    }

    LoadTsFileRateLimiter.getInstance().acquire(pieceNode.getDataSize());

    try {
      loadTsFileManager.writeToDataRegion(getDataRegion(dataRegionId), pieceNode, uuid);
    } catch (IOException e) {
      LOGGER.error(
          "IO error when writing piece node of TsFile {} to DataRegion {}.",
          pieceNode.getTsFile(),
          dataRegionId,
          e);
      status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    }

    return RpcUtils.SUCCESS_STATUS;
  }

  public TSStatus executeLoadCommand(
      LoadTsFileScheduler.LoadCommand loadCommand,
      String uuid,
      boolean isGeneratedByPipe,
      ProgressIndex progressIndex) {
    TSStatus status = new TSStatus();

    try {
      switch (loadCommand) {
        case EXECUTE:
          if (loadTsFileManager.loadAll(uuid, isGeneratedByPipe, progressIndex)) {
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
    } catch (Exception e) {
      LOGGER.error("Execute load command {} error.", loadCommand, e);
      status.setCode(TSStatusCode.LOAD_FILE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }

    return status;
  }

  /** reboot timed flush sequence/unsequence memtable thread */
  public void rebootTimedService() throws ShutdownException {
    LOGGER.info("Start rebooting all timed service.");

    // exclude ttl check thread
    stopTimedServiceAndThrow(seqMemtableTimedFlushCheckThread, "SeqMemtableTimedFlushCheckThread");
    stopTimedServiceAndThrow(
        unseqMemtableTimedFlushCheckThread, "UnseqMemtableTimedFlushCheckThread");

    LOGGER.info("Stop all timed service successfully, and now restart them.");

    startTimedService();

    LOGGER.info("Reboot all timed service successfully");
  }

  private void stopTimedServiceAndThrow(ScheduledExecutorService pool, String poolName)
      throws ShutdownException {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("{} still doesn't exit after 30s", poolName);
        throw new ShutdownException(e);
      }
    }
  }

  public void getDiskSizeByDataRegion(
      Map<Integer, Long> dataRegionDisk, List<Integer> dataRegionIds) {
    dataRegionMap.forEach(
        (dataRegionId, dataRegion) -> {
          if (dataRegionIds.contains(dataRegionId.getId())) {
            dataRegionDisk.put(dataRegionId.getId(), dataRegion.countRegionDiskSize());
          }
        });
  }

  public static File getDataRegionSystemDir(String dataBaseName, String dataRegionId) {
    return SystemFileFactory.INSTANCE.getFile(
        systemDir + File.separator + dataBaseName, dataRegionId);
  }

  public Runnable executeCompactFileTimeIndexCache() {
    return () -> {
      if (!isReadyForNonReadWriteFunctions()) {
        return;
      }
      for (DataRegion dataRegion : dataRegionMap.values()) {
        if (dataRegion != null) {
          dataRegion.compactFileTimeIndexCache();
        }
      }
    };
  }

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
