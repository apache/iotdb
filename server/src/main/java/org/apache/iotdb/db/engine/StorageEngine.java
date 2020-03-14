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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageEngine implements IService {

  private final Logger logger;
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir;

  /**
   * storage group name -> storage group processor
   */
  private final ConcurrentHashMap<String, StorageGroupProcessor> processorMap = new ConcurrentHashMap<>();

  private static final ExecutorService recoveryThreadPool = IoTDBThreadPoolFactory
      .newFixedThreadPool(Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");

  static class InstanceHolder {

    private InstanceHolder() {
      // forbidding instantiation
    }

    private static final StorageEngine INSTANCE = new StorageEngine();
  }

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private ScheduledExecutorService ttlCheckThread;
  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();

  /**
   * Time range for dividing storage group, the time unit is the same with IoTDB's TimestampPrecision
   */
  @ServerConfigConsistent
  static long timePartitionInterval;
  static {
    // build time Interval to divide time partition
    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        timePartitionInterval = IoTDBDescriptor.getInstance().
            getConfig().getPartitionInterval() * 1000_000_000L;
        break;
      case "us":
        timePartitionInterval = IoTDBDescriptor.getInstance().
            getConfig().getPartitionInterval() * 1000_000L;
        break;
      default:
        timePartitionInterval = IoTDBDescriptor.getInstance().
            getConfig().getPartitionInterval() * 1000;
        break;
    }
  }

  private StorageEngine() {
    logger = LoggerFactory.getLogger(StorageEngine.class);
    systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
    // recover upgrade process
    UpgradeUtils.recoverUpgrade();
    /*
     * recover all storage group processors.
     */

    List<StorageGroupMNode> sgNodes = MManager.getInstance().getAllStorageGroupNodes();
    List<Future> futures = new ArrayList<>();
    for (StorageGroupMNode storageGroup : sgNodes) {
      futures.add(recoveryThreadPool.submit((Callable<Void>) () -> {
        StorageGroupProcessor processor = new StorageGroupProcessor(systemDir,
            storageGroup.getFullPath(), fileFlushPolicy);
        processor.setDataTTL(storageGroup.getDataTTL());
        processorMap.put(storageGroup.getFullPath(), processor);
        logger.info("Storage Group Processor {} is recovered successfully",
            storageGroup.getFullPath());
        return null;
      }));
    }
    for (Future future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
      }
    }
  }

  @Override
  public void start() {
    ttlCheckThread = Executors.newSingleThreadScheduledExecutor();
    ttlCheckThread.scheduleAtFixedRate(this::checkTTL, TTL_CHECK_INTERVAL, TTL_CHECK_INTERVAL
        , TimeUnit.MILLISECONDS);
  }

  private void checkTTL() {
    try {
      for (StorageGroupProcessor processor : processorMap.values()) {
        processor.checkFilesTTL();
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("An error occurred when checking TTL", e);
    }
  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
    if (ttlCheckThread != null) {
      ttlCheckThread.shutdownNow();
      try {
        ttlCheckThread.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("TTL check thread still doesn't exit after 30s");
      }
    }
    recoveryThreadPool.shutdownNow();
    this.reset();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  public StorageGroupProcessor getProcessor(String path) throws StorageEngineException {
    String storageGroupName;
    try {
      storageGroupName = MManager.getInstance().getStorageGroupName(path);
      StorageGroupProcessor processor;
      processor = processorMap.get(storageGroupName);
      if (processor == null) {
        storageGroupName = storageGroupName.intern();
        synchronized (storageGroupName) {
          processor = processorMap.get(storageGroupName);
          if (processor == null) {
            logger.info("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroupName, Thread.currentThread().getId());
            processor = new StorageGroupProcessor(systemDir, storageGroupName, fileFlushPolicy);
            StorageGroupMNode storageGroup = MManager.getInstance()
                .getStorageGroupNode(storageGroupName);
            processor.setDataTTL(storageGroup.getDataTTL());
            processorMap.put(storageGroupName, processor);
          }
        }
      }
      return processor;
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }


  /**
   * This function is just for unit test.
   */
  public synchronized void reset() {
    processorMap.clear();
  }


  /**
   * insert an InsertPlan to a storage group.
   *
   * @param insertPlan physical plan of insertion
   */
  public void insert(InsertPlan insertPlan)
      throws StorageEngineException, QueryProcessException {

    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertPlan.getDeviceId());
    } catch (StorageEngineException e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}",
          insertPlan.getDeviceId(), e.getMessage(), e);
      throw new StorageEngineException(e);
    }

    // TODO monitor: update statistics
    try {
      storageGroupProcessor.insert(insertPlan);
    } catch (QueryProcessException e) {
      throw new QueryProcessException(e);
    }
  }

  /**
   * insert a BatchInsertPlan to a storage group
   *
   * @return result of each row
   */
  public TSStatus[] insertBatch(BatchInsertPlan batchInsertPlan) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(batchInsertPlan.getDeviceId());
    } catch (StorageEngineException e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}",
          batchInsertPlan.getDeviceId(),
          e.getMessage(), e);
      throw new StorageEngineException(e);
    }

    // TODO monitor: update statistics
    try {
      return storageGroupProcessor.insertBatch(batchInsertPlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * flush command Sync asyncCloseOneProcessor all file node processors.
   */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (StorageGroupProcessor processor : processorMap.values()) {
      processor.waitForAllCurrentTsFileProcessorsClosed();
      //TODO do we need to wait for all merging tasks to be finished here?
      processor.closeAllResources();
    }
  }

  public void asyncCloseProcessor(String storageGroupName, boolean isSeq)
      throws StorageGroupNotSetException {
    StorageGroupProcessor processor = processorMap.get(storageGroupName);
    if (processor != null) {
      logger.info("async closing sg processor is called for closing {}, seq = {}", storageGroupName,
          isSeq);
      processor.writeLock();
      try {
        if (isSeq) {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkSequenceTsFileProcessors())) {
            processor.moveOneWorkProcessorToClosingList(true, tsfileProcessor);
          }
        } else {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkUnsequenceTsFileProcessor())) {
            processor.moveOneWorkProcessorToClosingList(false, tsfileProcessor);
          }
        }
      } finally {
        processor.writeUnlock();
      }
    } else {
      throw new StorageGroupNotSetException(storageGroupName);
    }
  }

  /**
   * update data.
   */
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String v) {
    // TODO
  }

  /**
   * delete data of timeseries "{deviceId}.{measurementId}" with time <= timestamp.
   */
  public void delete(String deviceId, String measurementId, long timestamp)
      throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    try {
      storageGroupProcessor.delete(deviceId, measurementId, timestamp);
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context,
      QueryFileManager filePathsManager)
      throws StorageEngineException {
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor
        .query(deviceId, measurementId, context, filePathsManager, seriesExpression.getFilter());
  }

  /**
   * returns the top k% measurements that are recently used in queries.
   */
  public Set calTopKMeasurement(String deviceId, String sensorId, double k)
      throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.calTopKMeasurement(sensorId, k);
  }

  /**
   * count all Tsfiles which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded
   */
  public int countUpgradeFiles() {
    int totalUpgradeFileNum = 0;
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      totalUpgradeFileNum += storageGroupProcessor.countUpgradeFiles();
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
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      storageGroupProcessor.upgrade();
    }
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll(boolean fullMerge) throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      storageGroupProcessor.merge(fullMerge);
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneStorageGroup(String storageGroupName) {
    if (processorMap.containsKey(storageGroupName)) {
      syncDeleteDataFiles(storageGroupName);
    }
  }

  private void syncDeleteDataFiles(String storageGroupName) {
    logger.info("Force to delete the data in storage group processor {}", storageGroupName);
    StorageGroupProcessor processor = processorMap.get(storageGroupName);
    processor.syncDeleteDataFiles();
  }

  /**
   * add time series.
   */
  public void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(path.getDevice());
    storageGroupProcessor
        .addMeasurement(path.getMeasurement(), dataType, encoding, compressor, props);
  }

  public void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding)
      throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(path.getDevice());
    CompressionType compressor =
        TSFileDescriptor.getInstance().getConfig().getCompressor();
    storageGroupProcessor
        .addMeasurement(path.getMeasurement(), dataType, encoding, compressor,
            Collections.emptyMap());
  }


  /**
   * delete all data of storage groups' timeseries.
   */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (String storageGroup : MManager.getInstance().getAllStorageGroupNames()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    return true;
  }

  public void setTTL(String storageGroup, long dataTTL) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(storageGroup);
    storageGroupProcessor.setDataTTL(dataTTL);
  }

  public void deleteStorageGroup(String storageGroupName) {
    deleteAllDataFilesInOneStorageGroup(storageGroupName);
    StorageGroupProcessor processor = processorMap.remove(storageGroupName);
    if (processor != null) {
      processor.deleteFolder(systemDir);
    }
  }

  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws TsFileProcessorException, StorageEngineException {
    getProcessor(newTsFileResource.getFile().getParentFile().getName())
        .loadNewTsFileForSync(newTsFileResource);
  }

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws TsFileProcessorException, StorageEngineException, MetadataException {
    Map<String, Long> startTimeMap = newTsFileResource.getStartTimeMap();
    if (startTimeMap == null || startTimeMap.isEmpty()) {
      throw new StorageEngineException("Can not get the corresponding storage group.");
    }
    String device = startTimeMap.keySet().iterator().next();
    String storageGroupName = MManager.getInstance().getStorageGroupName(device);
    getProcessor(storageGroupName).loadNewTsFile(newTsFileResource);
  }

  public boolean deleteTsfileForSync(File deletedTsfile)
      throws StorageEngineException {
    return getProcessor(deletedTsfile.getParentFile().getName()).deleteTsfile(deletedTsfile);
  }

  public boolean deleteTsfile(File deletedTsfile) throws StorageEngineException {
    return getProcessor(getSgByEngineFile(deletedTsfile)).deleteTsfile(deletedTsfile);
  }

  public boolean moveTsfile(File tsfileToBeMoved, File targetDir)
      throws StorageEngineException, IOException {
    return getProcessor(getSgByEngineFile(tsfileToBeMoved)).moveTsfile(tsfileToBeMoved, targetDir);
  }

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @return sg name
   */
  private String getSgByEngineFile(File file) {
    return file.getParentFile().getParentFile().getName();
  }

  /**
   *
   * @return TsFiles (seq or unseq) grouped by their storage group and partition number.
   */
  public Map<String, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<String, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<String, StorageGroupProcessor> entry : processorMap.entrySet()) {
      List<TsFileResource> allResources = entry.getValue().getSequenceFileTreeSet();
      allResources.addAll(entry.getValue().getUnSequenceFileList());
      for (TsFileResource sequenceFile : allResources) {
        if (!sequenceFile.isClosed()) {
          continue;
        }
        String[] fileSplits = FilePathUtils.splitTsFilePath(sequenceFile);
        long partitionNum = Long.parseLong(fileSplits[fileSplits.length - 2]);
        Map<Long, List<TsFileResource>> storageGroupFiles = ret.computeIfAbsent(entry.getKey()
            ,n -> new HashMap<>());
        storageGroupFiles.computeIfAbsent(partitionNum, n -> new ArrayList<>()).add(sequenceFile);
      }
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(TsFileResource tsFileResource, String storageGroup) {
    // TODO-Cluster#350: integrate with time partitioning
    StorageGroupProcessor processor = processorMap.get(storageGroup);
    return processor != null && processor.isFileAlreadyExist(tsFileResource);
  }

  public static long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public static long fromTimeToTimePartition(long time) {

    return time / timePartitionInterval;
  }
}
