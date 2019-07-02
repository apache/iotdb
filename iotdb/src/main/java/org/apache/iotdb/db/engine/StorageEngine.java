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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
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

  private static final Logger logger = LoggerFactory
      .getLogger(StorageEngine.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private volatile boolean readOnly = false;

  /**
   * a folder (system/info/ by default) that persist system info. Ends with
   * File.separator Each StorageEngine will have a subfolder.
   */
  private final String infoDir;

  /**
   * storage group name -> storage group processor
   */
  private final ConcurrentHashMap<String, StorageGroupProcessor> processorMap = new ConcurrentHashMap<>();

  private static final StorageEngine INSTANCE = new StorageEngine();

  public static StorageEngine getInstance() {
    return INSTANCE;
  }

  private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;

  private enum FileNodeManagerStatus {
    NONE, MERGE, CLOSE
  }


  private StorageEngine() {
    infoDir = FilePathUtils.regularizePath(config.getSystemInfoDir());
    // create infoDir
    File dir = new File(infoDir);
    if (dir.mkdirs()) {
      logger.info("Information directory {} of all storage groups doesn't exist, create it",
          dir.getPath());
    }

    /**
     * recovery all file node processors.
     */
    try {
      List<String> storageGroups = MManager.getInstance().getAllFileNames();
      for (String storageGroup: storageGroups) {
        StorageGroupProcessor processor = new StorageGroupProcessor(infoDir, storageGroup);
        logger.info("Storage Group Processor {} is recovered successfully", storageGroup);
        processorMap.put(storageGroup, processor);
      }
    } catch (ProcessorException | MetadataErrorException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_NODE_SERVICE;
  }


  private StorageGroupProcessor getProcessor(String devicePath)
      throws StorageEngineException {
    String storageGroupName = "";
    try {
      // return the storage group name
      storageGroupName = MManager.getInstance().getStorageGroupNameByPath(devicePath);
      StorageGroupProcessor processor;
      processor = processorMap.get(storageGroupName);
      if (processor == null) {
        storageGroupName = storageGroupName.intern();
        synchronized (storageGroupName) {
          processor = processorMap.get(storageGroupName);
          if (processor == null) {
            logger.debug("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroupName, Thread.currentThread().getId());
            processor = new StorageGroupProcessor(infoDir, storageGroupName);
            synchronized (processorMap) {
              processorMap.put(storageGroupName, processor);
            }
          }
        }
      }
      return processor;
    } catch (PathErrorException | ProcessorException e) {
      logger.error("Fail to get StorageGroupProcessor {}", storageGroupName,  e);
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
   * insert TsRecord into storage group.
   *
   * @param insertPlan physical plan of insertion
   * @return true if and only if this insertion succeeds
   */
  public boolean insert(InsertPlan insertPlan) throws StorageEngineException {

    if (readOnly) {
      throw new StorageEngineException("Current system mode is read only, does not support insertion");
    }

    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertPlan.getDeviceId());
    } catch (Exception e) {
      logger.warn("get StorageGroupProcessor of device {} failed, because {}", insertPlan.getDeviceId(),
          e.getMessage(), e);
      throw new StorageEngineException(e);
    }

    // TODO monitor: update statistics
    return storageGroupProcessor.insert(insertPlan);
  }


  public void asyncFlushAndSealAllFiles() {
    synchronized (processorMap) {
      for (StorageGroupProcessor storageGroupProcessor : processorMap.values()) {
        storageGroupProcessor.asyncForceClose();
      }
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
   * delete data.
   */
  public void delete(String deviceId, String measurementId, long timestamp)
      throws StorageEngineException {

    if (readOnly) {
      throw new StorageEngineException("Current system mode is read only, does not support deletion");
    }

    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    try {
      storageGroupProcessor.delete(deviceId, measurementId, timestamp);
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }


  /**
   * begin query.
   *
   * @param deviceId queried deviceId
   * @return a query token for the device.
   */
  public int beginQuery(String deviceId) throws StorageEngineException {
    // TODO
    return -1;
  }

  /**
   * end query.
   */
  public void endQuery(String deviceId, int token) throws StorageEngineException {
    // TODO
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws StorageEngineException {
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.query(deviceId, measurementId);
  }

  /**
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param fileNodeName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public boolean appendFileToFileNode(String fileNodeName, TsFileResource appendFile,
      String appendFilePath) throws StorageEngineException {
    // TODO
    return true;
  }

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFiles(String storageGroupName, TsFileResource appendFile,
      String uuid) throws StorageEngineException {
    // TODO
    return null;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll() throws StorageEngineException {
    if (readOnly) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }
    // TODO
  }

  /**
   * Force to close the storage group processor.
   */
  public void deleteOneFileNodeTimeSeries(String processorName) {
    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
      return;
    }

    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
    if (processorMap.containsKey(processorName)) {
      deleteFileNodeBlocked(processorName);
    }
  }

  private void deleteFileNodeBlocked(String processorName) {
    logger.info("Force to delete the storage group processor {}", processorName);
    StorageGroupProcessor processor = processorMap.get(processorName);
    processor.syncCloseAndStopFileNode(() -> {
      try {
        // delete storage group data file
        for (String tsfilePath: DirectoryManager.getInstance().getAllTsFileFolders()) {
          File storageGroupFolder = new File(tsfilePath, processorName);
          if (storageGroupFolder.exists()) {
            FileUtils.deleteDirectory(storageGroupFolder);
          }
        }
        // delete storage group info file
        String fileNodePath = IoTDBDescriptor.getInstance().getConfig().getSystemInfoDir();
        fileNodePath = FilePathUtils.regularizePath(fileNodePath) + processorName;
        FileUtils.deleteDirectory(new File(fileNodePath));
      } catch (IOException e) {
        logger.error("Delete tsfiles failed", e);
      }
      synchronized (processorMap) {
        processorMap.remove(processorName);
      }
      return true;
    });
  }

  /**
   * add time series.
   */
  public void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props) throws StorageEngineException {
    StorageGroupProcessor storageGroupProcessor = getProcessor(path.getFullPath());
    storageGroupProcessor.addTimeSeries(path.getMeasurement(), dataType, encoding, compressor, props);
  }


  /**
   * delete all storage groups' timeseries.
   */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    // TODO
    return true;
  }

  /**
   * flush command
   * Sync asyncCloseOneProcessor all file node processors.
   */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    synchronized (processorMap){
      for(StorageGroupProcessor processor: processorMap.values()){
        processor.syncCloseFileNode();
      }
    }
  }

}
