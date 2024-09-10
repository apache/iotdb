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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.utils.fileTimeIndexCache.FileTimeIndexCacheWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.getFileTimeIndexSerializedSize;

public class FileTimeIndexCacheRecorder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTimeIndexCacheRecorder.class);

  private static final int VERSION = 0;

  protected static final String FILE_NAME = "FileTimeIndexCache_" + VERSION;

  private final ScheduledExecutorService recordFileIndexThread;

  private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

  private final Map<Integer, FileTimeIndexCacheWriter> writerMap = new ConcurrentHashMap<>();

  private FileTimeIndexCacheRecorder() {
    recordFileIndexThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.FILE_TIME_INDEX_RECORD.getName());
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        recordFileIndexThread, this::executeTasks, 100, 100, TimeUnit.MILLISECONDS);
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        recordFileIndexThread,
        StorageEngine.getInstance().executeCompactFileTimeIndexCache(),
        120_000L,
        120_000L,
        TimeUnit.MILLISECONDS);
  }

  private void executeTasks() {
    Runnable task;
    while ((task = taskQueue.poll()) != null) {
      recordFileIndexThread.submit(task);
    }
  }

  public void logFileTimeIndex(TsFileResource... tsFileResources) {
    if (tsFileResources != null && tsFileResources.length > 0) {
      TsFileResource firstResource = tsFileResources[0];
      TsFileID tsFileID = firstResource.getTsFileID();
      int dataRegionId = tsFileID.regionId;
      File dataRegionSysDir =
          StorageEngine.getDataRegionSystemDir(
              firstResource.getDatabaseName(), firstResource.getDataRegionId());
      FileTimeIndexCacheWriter writer = getWriter(dataRegionId, dataRegionSysDir);
      boolean result =
          taskQueue.offer(
              () -> {
                try {
                  ByteBuffer buffer =
                      ByteBuffer.allocate(
                          getFileTimeIndexSerializedSize() * tsFileResources.length);
                  for (TsFileResource tsFileResource : tsFileResources) {
                    tsFileResource.serializeFileTimeIndexToByteBuffer(buffer);
                  }
                  buffer.flip();
                  writer.write(buffer);
                } catch (IOException e) {
                  LOGGER.warn("Meet error when record FileTimeIndexCache: {}", e.getMessage());
                }
              });
      if (!result) {
        LOGGER.warn("Meet error when record FileTimeIndexCache");
      }
    }
  }

  public void compactFileTimeIndexIfNeeded(
      String dataBaseName,
      int dataRegionId,
      int currentResourceCount,
      Map<Long, TsFileResourceList> sequenceFiles,
      Map<Long, TsFileResourceList> unsequenceFiles) {
    FileTimeIndexCacheWriter writer =
        getWriter(
            dataRegionId,
            StorageEngine.getDataRegionSystemDir(dataBaseName, String.valueOf(dataRegionId)));

    if (writer.getLogFile().length()
        > currentResourceCount * getFileTimeIndexSerializedSize() * 100L) {

      boolean result =
          taskQueue.offer(
              () -> {
                try {
                  writer.clearFile();
                  for (TsFileResourceList sequenceList : sequenceFiles.values()) {
                    if (sequenceList != null && !sequenceList.isEmpty()) {
                      ByteBuffer buffer =
                          ByteBuffer.allocate(
                              getFileTimeIndexSerializedSize() * sequenceList.size());
                      for (TsFileResource tsFileResource : sequenceList) {
                        tsFileResource.serializeFileTimeIndexToByteBuffer(buffer);
                      }
                      buffer.flip();
                      writer.write(buffer);
                    }
                  }
                  for (TsFileResourceList unsequenceList : unsequenceFiles.values()) {
                    if (unsequenceList != null && !unsequenceList.isEmpty()) {
                      ByteBuffer buffer =
                          ByteBuffer.allocate(
                              getFileTimeIndexSerializedSize() * unsequenceList.size());
                      for (TsFileResource tsFileResource : unsequenceList) {
                        tsFileResource.serializeFileTimeIndexToByteBuffer(buffer);
                      }
                      buffer.flip();
                      writer.write(buffer);
                    }
                  }
                } catch (IOException e) {
                  LOGGER.warn("Meet error when compact FileTimeIndexCache: {}", e.getMessage());
                }
              });
      if (!result) {
        LOGGER.warn("Meet error when compact FileTimeIndexCache");
      }
    }
  }

  private FileTimeIndexCacheWriter getWriter(int dataRegionId, File dataRegionSysDir) {
    return writerMap.computeIfAbsent(
        dataRegionId,
        k -> {
          File logFile = SystemFileFactory.INSTANCE.getFile(dataRegionSysDir, FILE_NAME);
          try {
            if (!dataRegionSysDir.exists() && !dataRegionSysDir.mkdirs()) {
              LOGGER.debug(
                  "DataRegionSysDir has existed，filePath:{}", dataRegionSysDir.getAbsolutePath());
            }
            if (!logFile.createNewFile()) {
              LOGGER.debug("FileTimeIndex file has existed，filePath:{}", logFile.getAbsolutePath());
            }
            return new FileTimeIndexCacheWriter(logFile, true);
          } catch (IOException e) {
            LOGGER.error(
                "FileTimeIndex log file create filed，filePath:{}", logFile.getAbsolutePath(), e);
            throw new RuntimeException(e);
          }
        });
  }

  public void close() throws IOException {
    for (FileTimeIndexCacheWriter writer : writerMap.values()) {
      writer.close();
    }
  }

  public void removeFileTimeIndexCache(int dataRegionId) {
    FileTimeIndexCacheWriter writer = writerMap.remove(dataRegionId);
    if (writer != null) {
      try {
        writer.close();
        deleteFileOrDirectory(writer.getLogFile(), true);
      } catch (IOException e) {
        LOGGER.warn("Meet error when close FileTimeIndexCache: {}", e.getMessage());
      }
    }
  }

  public static FileTimeIndexCacheRecorder getInstance() {
    return FileTimeIndexCacheRecorder.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final FileTimeIndexCacheRecorder INSTANCE = new FileTimeIndexCacheRecorder();
  }
}
