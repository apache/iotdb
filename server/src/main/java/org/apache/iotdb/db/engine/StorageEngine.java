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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageEngineFailureException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.JobFileManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
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
  private static final long TTL_CHECK_INTERVAL = 60 * 1000;

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
  ;

  private static final StorageEngine INSTANCE = new StorageEngine();

  public static StorageEngine getInstance() {
    return INSTANCE;
  }

  private ScheduledExecutorService ttlCheckThread;

  private StorageEngine() {
    logger = LoggerFactory.getLogger(StorageEngine.class);
    systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException("create system directory failed!");
    }

    // recover upgrade process
    UpgradeUtils.recoverUpgrade();
    /*
     * recover all storage group processors.
     */
    List<MNode> sgNodes = MManager.getInstance().getAllStorageGroups();
    List<Future> futures = new ArrayList<>();
    for (MNode storageGroup : sgNodes) {
      futures.add(recoveryThreadPool.submit((Callable<Void>) () -> {
          StorageGroupProcessor processor = new StorageGroupProcessor(systemDir,storageGroup.getFullPath());
          processor.setDataTTL(storageGroup.getDataTTL());
          processorMap.put(storageGroup.getFullPath(), processor);
          logger.info("Storage Group Processor {} is recovered successfully",storageGroup.getFullPath());
        return null;
      }));
    }
    for (Future future: futures) {
      try {
        future.get();
      } catch (Exception e) {
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
    ttlCheckThread.shutdownNow();
    recoveryThreadPool.shutdownNow();
    try {
      ttlCheckThread.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("TTL check thread still doesn't exit after 30s");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  public StorageGroupProcessor getProcessor(String path) throws StorageEngineException {
    String storageGroupName = "";
    try {
      storageGroupName = MManager.getInstance().getStorageGroupNameByPath(path);
      StorageGroupProcessor processor;
      processor = processorMap.get(storageGroupName);
      if (processor == null) {
        storageGroupName = storageGroupName.intern();
        synchronized (storageGroupName) {
          processor = processorMap.get(storageGroupName);
          if (processor == null) {
            logger.info("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroupName, Thread.currentThread().getId());
            processor = new StorageGroupProcessor(systemDir, storageGroupName);
            processor.setDataTTL(
                MManager.getInstance().getNodeByPathWithCheck(storageGroupName).getDataTTL());
            processorMap.put(storageGroupName, processor);
          }
        }
      }
      return processor;
    } catch (StorageGroupException | ProcessorException | PathErrorException e) {
      logger.error("Fail to get StorageGroupProcessor {}", storageGroupName, e);
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
  public void insert(InsertPlan insertPlan) throws ProcessorException {

    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertPlan.getDeviceId());
    } catch (Exception e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}",
          insertPlan.getDeviceId(),
          e.getMessage(), e);
      throw new ProcessorException(e);
    }

    // TODO monitor: update statistics
    try {
      storageGroupProcessor.insert(insertPlan);
    } catch (QueryProcessorException e) {
      throw new ProcessorException(e);
    }
  }

  /**
   * insert a BatchInsertPlan to a storage group
   *
   * @return result of each row
   */
  public Integer[] insertBatch(BatchInsertPlan batchInsertPlan) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(batchInsertPlan.getDeviceId());
    } catch (Exception e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}",
          batchInsertPlan.getDeviceId(),
          e.getMessage(), e);
      throw new StorageEngineException(e);
    }

    // TODO monitor: update statistics
    try {
      return storageGroupProcessor.insertBatch(batchInsertPlan);
    } catch (QueryProcessorException e) {
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
      throw new StorageEngineException(e);
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context,
      JobFileManager filePathsManager)
      throws StorageEngineException {
    //TODO use context.
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.query(deviceId, measurementId, context, filePathsManager);
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
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile       the appended tsfile information
   */
  @SuppressWarnings("unused") // reimplement sync module
  public boolean appendFileToStorageGroupProcessor(String storageGroupName,
      TsFileResource appendFile,
      String appendFilePath) throws StorageEngineException {
    // TODO reimplement sync module
    return true;
  }

  /**
   * get all overlap TsFiles which are conflict with the appendFile.
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile       the appended tsfile information
   */
  @SuppressWarnings("unused") // reimplement sync module
  public List<String> getOverlapFiles(String storageGroupName, TsFileResource appendFile,
      String uuid) throws StorageEngineException {
    // TODO reimplement sync module
    return Collections.emptyList();
  }

  /**
   * count all Tsfiles which need to be upgraded
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


  /**
   * delete all data of storage groups' timeseries.
   */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
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

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws TsFileProcessorException, StorageEngineException {
    getProcessor(newTsFileResource.getFile().getParentFile().getName())
        .loadNewTsFile(newTsFileResource);
  }

  public void deleteTsfile(File deletedTsfile) throws StorageEngineException {
    getProcessor(deletedTsfile.getParentFile().getName()).deleteTsfile(deletedTsfile);
  }

}
