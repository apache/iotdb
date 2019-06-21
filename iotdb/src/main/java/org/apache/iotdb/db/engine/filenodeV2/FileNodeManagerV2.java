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
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessor;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeManagerV2 implements IService {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(org.apache.iotdb.db.engine.filenodeV2.FileNodeManagerV2.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * a folder (system/info/ by default) that persist FileNodeProcessorStore classes. Ends with
   * File.separator Each FileNodeManager will have a subfolder.
   */
  private final String baseDir;

  /**
   * This map is used to manage all filenode processor,<br> the key is filenode name which is
   * storage group seriesPath.
   */
  private final ConcurrentHashMap<String, FileNodeProcessorV2> processorMap = new ConcurrentHashMap<>();

  private static final FileNodeManagerV2 INSTANCE = new FileNodeManagerV2();

  public static FileNodeManagerV2 getInstance() {
    return INSTANCE;
  }

  /**
   * This set is used to store overflowed filenode name.<br> The overflowed filenode will be merge.
   */
  private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;

  private enum FileNodeManagerStatus {
    NONE, MERGE, CLOSE
  }


  private FileNodeManagerV2() {
    baseDir = FilePathUtils.regularizePath(config.getFileNodeDir());
    // create baseDir
    File dir = new File(baseDir);
    if (dir.mkdirs()) {
      LOGGER.info("Base directory {} of all storage groups doesn't exist, create it", dir.getPath());
    }
  }

  @Override
  public void start() throws StartupException {

  }

  @Override
  public void stop() {
    try {
      syncCloseAllProcessor();
    } catch (FileNodeManagerException e) {
      LOGGER.error("Failed to setCloseMark file node manager because .", e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_NODE_SERVICE;
  }


  private FileNodeProcessorV2 getProcessor(String devicePath)
      throws FileNodeManagerException {
    String storageGroup = "";
    try {
      // return the storage group name
      storageGroup = MManager.getInstance().getFileNameByPath(devicePath);
      FileNodeProcessorV2 processor;
      processor = processorMap.get(storageGroup);
      if (processor == null) {
        storageGroup = storageGroup.intern();
        synchronized (storageGroup) {
          processor = processorMap.get(storageGroup);
          if (processor == null) {
            LOGGER.debug("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroup, Thread.currentThread().getId());
            processor = new FileNodeProcessorV2(baseDir, storageGroup);
            synchronized (processorMap) {
              processorMap.put(storageGroup, processor);
            }
          }
        }
      }
      return processor;
    } catch (PathErrorException e) {
      LOGGER.error("MManager get storage group name error, seriesPath is {}", devicePath);
      throw new FileNodeManagerException(e);
    } catch (FileNodeProcessorException e) {
      LOGGER.error("Fail to init simple file version controller of file node {}", storageGroup,  e);
      throw new FileNodeManagerException(e);
    }
  }


  /**
   * This function is just for unit test.
   */
  public synchronized void resetFileNodeManager() {
    processorMap.clear();
  }


  /**
   * insert TsRecord into storage group.
   *
   * @param tsRecord input Data
   * @return an int value represents the insert type, 0: failed; 1: overflow; 2: bufferwrite
   */
  public int insert(TSRecord tsRecord) throws FileNodeManagerException {

    FileNodeProcessorV2 fileNodeProcessor;
    try {
      fileNodeProcessor = getProcessor(tsRecord.deviceId);
    } catch (Exception e) {
      LOGGER.warn("get FileNodeProcessor of device {} failed, because {}", tsRecord.deviceId,
          e.getMessage(), e);
      throw new FileNodeManagerException(e);
    }

    // TODO monitor: update statistics
    return fileNodeProcessor.insert(tsRecord);
  }


  public void asyncFlushAndSealAllFiles() {
    synchronized (processorMap) {
      for (FileNodeProcessorV2 fileNodeProcessor: processorMap.values()) {
        fileNodeProcessor.asyncForceClose();
      }
    }
  }


  /**
   * recovery the filenode processor.
   */
  public void recovery() {
    // TODO
  }


  private void writeLog(TSRecord tsRecord, boolean isMonitor, WriteLogNode logNode)
      throws FileNodeManagerException {
    // TODO
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
      throws FileNodeManagerException {
    // TODO
  }

  private void delete(String processorName,
      Iterator<Entry<String, FileNodeProcessor>> processorIterator)
      throws FileNodeManagerException {
    // TODO
  }


  /**
   * begin query.
   *
   * @param deviceId queried deviceId
   * @return a query token for the device.
   */
  public int beginQuery(String deviceId) throws FileNodeManagerException {
    // TODO
    return -1;
  }

  /**
   * end query.
   */
  public void endQuery(String deviceId, int token) throws FileNodeManagerException {
    // TODO
  }

  /**
   * query data.
   */
  public QueryDataSourceV2 query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws FileNodeManagerException {
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
    FileNodeProcessorV2 fileNodeProcessor = getProcessor(deviceId);
    return fileNodeProcessor.query(deviceId, measurementId);
  }

  /**
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param fileNodeName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public boolean appendFileToFileNode(String fileNodeName, TsFileResource appendFile,
      String appendFilePath) throws FileNodeManagerException {
    // TODO
    return true;
  }

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param fileNodeName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFilesFromFileNode(String fileNodeName, TsFileResource appendFile,
      String uuid) throws FileNodeManagerException {
    // TODO
    return null;
  }


  /**
   * merge all overflowed filenode.
   *
   * @throws FileNodeManagerException FileNodeManagerException
   */
  public void mergeAll() throws FileNodeManagerException {
    // TODO
  }

  /**
   * try to setCloseMark the filenode processor. The name of filenode processor is processorName
   */
  private boolean tryToCloseFileNodeProcessor(String processorName) throws FileNodeManagerException {
    // TODO
    return false;
  }

  /**
   * Force to setCloseMark the filenode processor.
   */
  public void deleteOneFileNode(String processorName) throws FileNodeManagerException {
    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
      return;
    }

    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
    try {
      if (processorMap.containsKey(processorName)) {
        deleteFileNodeBlocked(processorName);
      }
    } catch (IOException e) {
      LOGGER.error("Delete the filenode processor {} error.", processorName, e);
      throw new FileNodeManagerException(e);
    } finally {
      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
    }
  }

  private void deleteFileNodeBlocked(String processorName) throws IOException {
    LOGGER.info("Forced to delete the filenode processor {}", processorName);
    FileNodeProcessorV2 processor = processorMap.get(processorName);
    processor.syncCloseAndStopFileNode(() -> {
      String fileNodePath = IoTDBDescriptor.getInstance().getConfig().getFileNodeDir();
      fileNodePath = FilePathUtils.regularizePath(fileNodePath) + processorName;
      try {
        FileUtils.deleteDirectory(new File(fileNodePath));
      } catch (IOException e) {
        LOGGER.error("Delete tsfiles failed", e);
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
      Map<String, String> props) throws FileNodeManagerException {
    FileNodeProcessorV2 fileNodeProcessor = getProcessor(path.getFullPath());
    fileNodeProcessor.addTimeSeries(path.getMeasurement(), dataType, encoding, compressor, props);
  }


  /**
   * delete all filenode.
   */
  public synchronized boolean deleteAll() throws FileNodeManagerException {
    LOGGER.info("Start deleting all filenode");
    // TODO
    return true;
  }

  /**
   * Sync asyncCloseOneProcessor all file node processors.
   */
  public void syncCloseAllProcessor() throws FileNodeManagerException {
    LOGGER.info("Start closing all filenode processor");
    synchronized (processorMap){
      for(FileNodeProcessorV2 processor: processorMap.values()){
        processor.asyncForceClose();
      }
    }
  }

}
