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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeManagerV2 implements IService {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(org.apache.iotdb.db.engine.filenodeV2.FileNodeManagerV2.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Directories directories = Directories.getInstance();

  /**
   * a folder (system/info/ by default) that persist FileNodeProcessorStore classes. Ends with
   * File.separator Each FileNodeManager will have a subfolder.
   */
  private final String baseDir;

  /**
   * This map is used to manage all filenode processor,<br> the key is filenode name which is
   * storage group seriesPath.
   */
  private ConcurrentHashMap<String, FileNodeProcessorV2> processorMap;

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
    String normalizedBaseDir = config.getFileNodeDir();
    if (normalizedBaseDir.charAt(normalizedBaseDir.length() - 1) != File.separatorChar) {
      normalizedBaseDir += Character.toString(File.separatorChar);
    }
    baseDir = normalizedBaseDir;
    processorMap = new ConcurrentHashMap<>();

    // create baseDir
    File dir = new File(baseDir);
    if (dir.mkdirs()) {
      LOGGER.info("baseDir {} doesn't exist, create it", dir.getPath());
    }

  }

  @Override
  public void start() throws StartupException {

  }

  @Override
  public void stop() {

  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_NODE_SERVICE;
  }


  private FileNodeProcessorV2 getProcessor(String devicePath)
      throws FileNodeManagerException, FileNodeProcessorException {
    String filenodeName;
    try {
      // return the storage group name
      filenodeName = MManager.getInstance().getFileNameByPath(devicePath);
    } catch (PathErrorException e) {
      LOGGER.error("MManager get storage group name error, seriesPath is {}", devicePath);
      throw new FileNodeManagerException(e);
    }
    FileNodeProcessorV2 processor;
    processor = processorMap.get(filenodeName);
    if (processor == null) {
      filenodeName = filenodeName.intern();
      synchronized (filenodeName) {
        processor = processorMap.get(filenodeName);
        if (processor == null) {
          LOGGER.debug("construct a processor instance, the storage group is {}, Thread is {}",
              filenodeName, Thread.currentThread().getId());
          processor = new FileNodeProcessorV2(baseDir, filenodeName);
          processorMap.put(filenodeName, processor);
        }
      }
    }
    return processor;
  }


  /**
   * insert TsRecord into storage group.
   *
   * @param tsRecord input Data
   * @return an int value represents the insert type, 0: failed; 1: overflow; 2: bufferwrite
   */
  public boolean insert(TSRecord tsRecord) {

    FileNodeProcessorV2 fileNodeProcessor;
    try {
      fileNodeProcessor = getProcessor(tsRecord.deviceId);
    } catch (Exception e) {
      LOGGER.warn("get FileNodeProcessor of device {} failed, because {}", tsRecord.deviceId,
          e.getMessage(), e);
      return false;
    }

    return fileNodeProcessor.insert(tsRecord);
  }

  private void closeAllFileNodeProcessor() {
    synchronized (processorMap) {
      LOGGER.info("Start to setCloseMark all FileNode");
      if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
        LOGGER.info(
            "Failed to setCloseMark all FileNode processor because the FileNodeManager's status is {}",
            fileNodeManagerStatus);
        return;
      }

      fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;

      for (Map.Entry<String, FileNodeProcessorV2> processorEntry : processorMap.entrySet()) {

      }

    }
  }

}
