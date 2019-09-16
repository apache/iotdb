/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.JobFileManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageEngine implements IService {

  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir;

  /**
   * storage group name -> storage group processor
   */
  private final ConcurrentHashMap<String, StorageGroupProcessor> processorMap = new ConcurrentHashMap<>();

  private static final StorageEngine INSTANCE = new StorageEngine();

  public static StorageEngine getInstance() {
    return INSTANCE;
  }

  private StorageEngine() {
    systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    // create systemDir
    try {
      FileUtils.forceMkdir(new File(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException("create system directory failed!");
    }

    /**
     * recover all storage group processors.
     */
    try {
      List<MNode> sgNodes = MManager.getInstance().getAllStorageGroups();
      for (MNode storageGroup : sgNodes) {
        StorageGroupProcessor processor = new StorageGroupProcessor(systemDir, storageGroup.getFullPath());
        processor.setDataTTL(storageGroup.getDataTTL());
        logger.info("Storage Group Processor {} is recovered successfully", storageGroup.getFullPath());
        processorMap.put(storageGroup.getFullPath(), processor);
      }
    } catch (ProcessorException e) {
      logger.error("init a storage group processor failed. ", e);
      throw new StorageEngineFailureException(e);
    }
  }

  @Override
  public void start() {
    // nothing to be done
  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }


  private StorageGroupProcessor getProcessor(String path) throws StorageEngineException {
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
            logger.debug("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroupName, Thread.currentThread().getId());
            processor = new StorageGroupProcessor(systemDir, storageGroupName);
            processorMap.put(storageGroupName, processor);
          }
        }
      }
      return processor;
    } catch (PathErrorException | ProcessorException e) {
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
   * @return true if and only if this insertion succeeds
   */
  public boolean insert(InsertPlan insertPlan) throws StorageEngineException {

    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertPlan.getDeviceId());
    } catch (Exception e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}",
          insertPlan.getDeviceId(),
          e.getMessage(), e);
      throw new StorageEngineException(e);
    }

    // TODO monitor: update statistics
    return storageGroupProcessor.insert(insertPlan);
  }

  /**
   * insert a BatchInsertPlan to a storage group
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
    return storageGroupProcessor.insertBatch(batchInsertPlan);
  }

  /**
   * only for unit test
   */
  public void asyncFlushAndSealAllFiles() {
    for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
      storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();
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
   * @param appendFile the appended tsfile information
   */
  @SuppressWarnings("unused") // reimplement sync module
  public boolean appendFileToStorageGroupProcessor(String storageGroupName,
      TsFileResource appendFile,
      String appendFilePath) throws StorageEngineException {
    // TODO reimplement sync module
    return true;
  }

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  @SuppressWarnings("unused") // reimplement sync module
  public List<String> getOverlapFiles(String storageGroupName, TsFileResource appendFile,
      String uuid) throws StorageEngineException {
    // TODO reimplement sync module
    return Collections.emptyList();
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
}
