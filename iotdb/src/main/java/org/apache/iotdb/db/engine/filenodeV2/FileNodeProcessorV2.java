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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.bufferwriteV2.BufferWriteProcessorV2;
import org.apache.iotdb.db.engine.filenode.CopyOnWriteLinkedList;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessorV2.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final String RESTORE_FILE_SUFFIX = ".restore";

  private static final MManager mManager = MManager.getInstance();
  private static final Directories directories = Directories.getInstance();

  private FileSchema fileSchema;

  /**
   * device -> tsfile list
   */
  private Map<String, List<TsFileResourceV2>> invertedIndexOfFiles = new HashMap<>();

  private List<TsFileResourceV2> newFileNodes;

  private BufferWriteProcessorV2 workBufferWriteProcessor = null;

  /**
   * device -> global latest timestamp of each device
   */
  private Map<String, Long> latestTimeMap = new HashMap<>();

  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncFlush
   */
  private Map<String, Long> latestFlushTimeMap = new HashMap<>();

  private String storageGroup;

  private CopyOnWriteLinkedList<BufferWriteProcessorV2> closingBufferWriteProcessor = new CopyOnWriteLinkedList<>();

  private final ReadWriteLock lock;

  private VersionController versionController;

  private String fileNodeRestoreFilePath;
  private FileNodeProcessorStoreV2 fileNodeProcessorStore;
  private final Object fileNodeRestoreLock = new Object();


  public FileNodeProcessorV2(String baseDir, String storageGroup) throws FileNodeProcessorException {
    this.storageGroup = storageGroup;
    lock = new ReentrantReadWriteLock();

    File storageGroupDir = new File(baseDir + storageGroup);
    if (!storageGroupDir.exists()) {
      storageGroupDir.mkdir();
      LOGGER.info(
          "The directory of the storage group {} doesn't exist. Create a new " +
              "directory {}", storageGroup, storageGroupDir.getAbsolutePath());
    }

    /**
     * restore
     */
    File restoreFolder = new File(baseDir + storageGroup);
    if (!restoreFolder.exists()) {
      restoreFolder.mkdirs();
      LOGGER.info("The restore directory of the filenode processor {} doesn't exist. Create new " +
              "directory {}", storageGroup, restoreFolder.getAbsolutePath());
    }
    fileNodeRestoreFilePath = new File(restoreFolder, storageGroup + RESTORE_FILE_SUFFIX)
        .getPath();
    try {
      fileNodeProcessorStore = readStoreFromDisk();
    } catch (FileNodeProcessorException e) {
      LOGGER.error(
          "The fileNode processor {} encountered an error when recoverying restore " +
              "information.", storageGroup);
      throw new FileNodeProcessorException(e);
    }
    // TODO deep clone the lastupdate time, change the getNewFileNodes to V2
//    newFileNodes = fileNodeProcessorStore.getNewFileNodes();
    invertedIndexOfFiles = new HashMap<>();

    /**
     * version controller
     */
    try {
      versionController = new SimpleFileVersionController(restoreFolder.getPath());
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }

  }


  private FileNodeProcessorStoreV2 readStoreFromDisk() throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      File restoreFile = new File(fileNodeRestoreFilePath);
      if (!restoreFile.exists() || restoreFile.length() == 0) {
        return new FileNodeProcessorStoreV2(false, new HashMap<>(),
            new ArrayList<>(), FileNodeProcessorStatus.NONE, 0);
      }
      try (FileInputStream inputStream = new FileInputStream(fileNodeRestoreFilePath)) {
        return FileNodeProcessorStoreV2.deSerialize(inputStream);
      } catch (IOException e) {
        LOGGER.error("Failed to deserialize the FileNodeRestoreFile {}, {}", fileNodeRestoreFilePath,
                e);
        throw new FileNodeProcessorException(e);
      }
    }
  }

  private void writeStoreToDisk(FileNodeProcessorStoreV2 fileNodeProcessorStore)
      throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      try (FileOutputStream fileOutputStream = new FileOutputStream(fileNodeRestoreFilePath)) {
        fileNodeProcessorStore.serialize(fileOutputStream);
        LOGGER.debug("The filenode processor {} writes restore information to the restore file",
            storageGroup);
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }
    }
  }


  public boolean insert(TSRecord tsRecord) {
    lock.writeLock().lock();
    boolean result = true;

    try {

      // create a new BufferWriteProcessor
      if (workBufferWriteProcessor == null) {
        String baseDir = directories.getNextFolderForTsfile();
        String filePath = Paths.get(baseDir, storageGroup, tsRecord.time + "").toString();
        workBufferWriteProcessor = new BufferWriteProcessorV2(storageGroup, new File(filePath),
            fileSchema, versionController, this::closeBufferWriteProcessorCallBack);
        newFileNodes.add(workBufferWriteProcessor.getTsFileResource());
        // TODO check if the disk is full
      }

      // init map
      latestTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);
      latestFlushTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);

      if (tsRecord.time > latestFlushTimeMap.get(tsRecord.deviceId)) {

        // write BufferWrite
        result = workBufferWriteProcessor.write(tsRecord);

        // try to update the latest time of the device of this tsRecord
        if (result && latestTimeMap.get(tsRecord.deviceId) < tsRecord.time) {
          latestTimeMap.put(tsRecord.deviceId, tsRecord.time);
        }

        // check memtable size and may asyncFlush the workMemtable
        if (workBufferWriteProcessor.shouldFlush()) {
          flushAndCheckClose();
        }

      } else {
        // TODO write to overflow
      }
    } catch (Exception e) {

    } finally {
      lock.writeLock().unlock();
    }

    return result;
  }

  /**
   * ensure there must be a flush thread submitted after close() is called,
   * therefore the close task will be executed by a flush thread.
   * -- said by qiaojialin
   *
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void flushAndCheckClose() {
    boolean shouldClose = false;
    // check file size and may close the BufferWrite
    if (workBufferWriteProcessor.shouldClose()) {
      closingBufferWriteProcessor.add(workBufferWriteProcessor);
      workBufferWriteProcessor.close();
      shouldClose = true;
    }

    workBufferWriteProcessor.asyncFlush();

    if (shouldClose) {
      workBufferWriteProcessor = null;
    }

    // update the largest timestamp in the last flushing memtable
    for (Entry<String, Long> entry : latestTimeMap.entrySet()) {
      latestFlushTimeMap.put(entry.getKey(), entry.getValue());
    }
  }


  /**
   * return the memtable to MemTablePool and make metadata in writer visible
   */
  private void closeBufferWriteProcessorCallBack(Object bufferWriteProcessor) {
    closingBufferWriteProcessor.remove((BufferWriteProcessorV2) bufferWriteProcessor);
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setLastUpdateTimeMap(latestTimeMap);

      if (!newFileNodes.isEmpty()) {
        // end time with one start time
        Map<String, Long> endTimeMap = new HashMap<>();
        TsFileResourceV2 resource = workBufferWriteProcessor.getTsFileResource();
        for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
          String deviceId = startTime.getKey();
          endTimeMap.put(deviceId, latestTimeMap.get(deviceId));
        }
        resource.setEndTimeMap(endTimeMap);
      }
      fileNodeProcessorStore.setNewFileNodes(newFileNodes);
      try {
        writeStoreToDisk(fileNodeProcessorStore);
      } catch (FileNodeProcessorException e) {
        LOGGER.error("write FileNodeStore info error, because {}", e.getMessage(), e);
      }
    }
  }

  public void forceClose() {
    lock.writeLock().lock();
    try {
      if (workBufferWriteProcessor != null) {
        closingBufferWriteProcessor.add(workBufferWriteProcessor);
        workBufferWriteProcessor.forceClose();
        workBufferWriteProcessor = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
