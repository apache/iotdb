/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.sync.receiver.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.sync.sender.conf.SyncConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLoader implements IFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

  private static final int WAIT_TIME = 100;

  private String syncFolderPath;

  private String senderName;

  private BlockingQueue<LoadTask> queue = new LinkedBlockingQueue<>();

  private LoadLogger loadLog;

  private LoadType curType = LoadType.NONE;

  private volatile boolean endSync = false;

  private FileLoader(String senderName, String syncFolderPath) throws IOException {
    this.senderName = senderName;
    this.syncFolderPath = syncFolderPath;
    this.loadLog = new LoadLogger(new File(syncFolderPath, SyncConstant.LOAD_LOG_NAME));
    FileLoaderManager.getInstance().addFileLoader(senderName, this);
    FileLoaderManager.getInstance().addLoadTaskRunner(loadTaskRunner);
  }

  public static FileLoader createFileLoader(String senderName, String syncFolderPath)
      throws IOException {
    return new FileLoader(senderName, syncFolderPath);
  }

  public static FileLoader createFileLoader(File syncFolder)
      throws IOException {
    return new FileLoader(syncFolder.getName(), syncFolder.getAbsolutePath());
  }

  private Runnable loadTaskRunner = () -> {
    try {
      while (true) {
        if (queue.isEmpty() && endSync) {
          cleanUp();
          break;
        }
        LoadTask loadTask = queue.poll(WAIT_TIME, TimeUnit.MILLISECONDS);
        if (loadTask != null) {
          try {
            handleLoadTask(loadTask);
          } catch (Exception e) {
            LOGGER.error("Can not load task {}", loadTask, e);
          }
        }
      }
    } catch (InterruptedException e) {
      LOGGER.error("Can not handle load task", e);
    }
  };

  @Override
  public void addDeletedFileName(File deletedFile) {
    queue.add(new LoadTask(deletedFile, LoadType.DELETE));
  }

  @Override
  public void addTsfile(File tsfile) {
    queue.add(new LoadTask(tsfile, LoadType.ADD));
  }

  @Override
  public void endSync() {
    if (!endSync && FileLoaderManager.getInstance().containsFileLoader(senderName)) {
      this.endSync = true;
    }
  }

  @Override
  public void handleLoadTask(LoadTask task) throws IOException {
    switch (task.type) {
      case ADD:
        loadNewTsfile(task.file);
        break;
      case DELETE:
        loadDeletedFile(task.file);
        break;
      default:
        LOGGER.error("Wrong load task type {}", task.type);
    }
  }

  private void loadNewTsfile(File newTsFile) throws IOException {
    if (curType != LoadType.ADD) {
      loadLog.startLoadTsFiles();
      curType = LoadType.ADD;
    }
    if (!newTsFile.exists()) {
      LOGGER.info("Tsfile {} doesn't exist.", newTsFile.getAbsolutePath());
      return;
    }
    TsFileResource tsFileResource = new TsFileResource(new File(newTsFile.getAbsolutePath()));
    checkTsFileResource(tsFileResource);
    try {
      StorageEngine.getInstance().loadNewTsFile(newTsFile, tsFileResource);
    } catch (TsFileProcessorException | StorageEngineException e) {
      LOGGER.error("Can not load new tsfile {}", newTsFile.getAbsolutePath(), e);
      throw new IOException(e);
    }
    loadLog.finishLoadDeletedFile(newTsFile);
  }

  private void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.fileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getFile().getAbsolutePath())) {
        TsFileMetaData metaData = reader.readFileMetadata();
        List<TsDeviceMetadataIndex> deviceMetadataIndexList = new ArrayList<>(
            metaData.getDeviceMap().values());
        for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
          TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
          List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata
              .getChunkGroupMetaDataList();
          for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
            for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
              tsFileResource.updateStartTime(chunkGroupMetaData.getDeviceID(),
                  chunkMetaData.getStartTime());
              tsFileResource
                  .updateEndTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
            }
          }
        }
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deSerialize();
    }
  }

  private void loadDeletedFile(File deletedTsFile) throws IOException {
    if (curType != LoadType.DELETE) {
      loadLog.startLoadDeletedFiles();
      curType = LoadType.DELETE;
    }
    try {
      StorageEngine.getInstance().deleteTsfile(deletedTsFile);
    } catch (StorageEngineException e) {
      LOGGER.error("Can not load deleted tsfile {}", deletedTsFile.getAbsolutePath(), e);
      throw new IOException(e);
    }
    loadLog.finishLoadTsfile(deletedTsFile);
  }


  @Override
  public void cleanUp() {
    try {
      loadLog.close();
      new File(syncFolderPath, SyncConstant.SYNC_LOG_NAME).delete();
      new File(syncFolderPath, SyncConstant.LOAD_LOG_NAME).delete();
      FileUtils.deleteDirectory(new File(syncFolderPath, SyncConstant.RECEIVER_DATA_FOLDER_NAME));
      FileLoaderManager.getInstance().removeFileLoader(senderName);
    } catch (IOException e) {
      LOGGER.error("Can not clean up sync resource.", e);
    }
  }

  public void setCurType(LoadType curType) {
    this.curType = curType;
  }

  class LoadTask {

    private File file;
    private LoadType type;

    LoadTask(File file, LoadType type) {
      this.file = file;
      this.type = type;
    }

    @Override
    public String toString() {
      return "LoadTask{" +
          "file=" + file +
          ", type=" + type +
          '}';
    }
  }
}
