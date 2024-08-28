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

import static org.apache.iotdb.commons.utils.FileUtils.deleteDirectoryAndEmptyParent;
import static org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.getFileTimeIndexSerializedSize;

public class FileTimeIndexCacheRecorder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTimeIndexCacheRecorder.class);

  private static final int VERSION = 0;

  protected static final String FILE_NAME = "FileTimeIndexCache_" + VERSION;

  private final ScheduledExecutorService recordFileIndexThread;

  private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

  private final Map<Integer, Map<Long, FileTimeIndexCacheWriter>> writerMap =
      new ConcurrentHashMap<>();

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
      long partitionId = tsFileID.timePartitionId;
      File dataRegionSysDir =
          StorageEngine.getDataRegionSystemDir(
              firstResource.getDatabaseName(), firstResource.getDataRegionId());
      FileTimeIndexCacheWriter writer = getWriter(dataRegionId, partitionId, dataRegionSysDir);
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
      long partitionId,
      TsFileResourceList sequenceFiles,
      TsFileResourceList unsequenceFiles) {
    FileTimeIndexCacheWriter writer =
        getWriter(
            dataRegionId,
            partitionId,
            StorageEngine.getDataRegionSystemDir(dataBaseName, String.valueOf(dataRegionId)));

    int currentResourceCount =
        (sequenceFiles == null ? 0 : sequenceFiles.size())
            + (unsequenceFiles == null ? 0 : unsequenceFiles.size());
    if (writer.getLogFile().length()
        > currentResourceCount * getFileTimeIndexSerializedSize() * 100L) {

      boolean result =
          taskQueue.offer(
              () -> {
                try {
                  writer.clearFile();
                  if (sequenceFiles != null && !sequenceFiles.isEmpty()) {
                    ByteBuffer buffer =
                        ByteBuffer.allocate(
                            getFileTimeIndexSerializedSize() * sequenceFiles.size());
                    for (TsFileResource tsFileResource : sequenceFiles) {
                      tsFileResource.serializeFileTimeIndexToByteBuffer(buffer);
                    }
                    buffer.flip();
                    writer.write(buffer);
                  }
                  if (unsequenceFiles != null && !unsequenceFiles.isEmpty()) {
                    ByteBuffer buffer =
                        ByteBuffer.allocate(
                            getFileTimeIndexSerializedSize() * unsequenceFiles.size());
                    for (TsFileResource tsFileResource : unsequenceFiles) {
                      tsFileResource.serializeFileTimeIndexToByteBuffer(buffer);
                    }
                    buffer.flip();
                    writer.write(buffer);
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

  private FileTimeIndexCacheWriter getWriter(
      int dataRegionId, long partitionId, File dataRegionSysDir) {
    return writerMap
        .computeIfAbsent(dataRegionId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(
            partitionId,
            k -> {
              File partitionDir =
                  SystemFileFactory.INSTANCE.getFile(dataRegionSysDir, String.valueOf(partitionId));
              File logFile = SystemFileFactory.INSTANCE.getFile(partitionDir, FILE_NAME);
              try {
                if (!partitionDir.exists() && !partitionDir.mkdirs()) {
                  LOGGER.debug(
                      "Partition directory has existed，filePath:{}",
                      partitionDir.getAbsolutePath());
                }
                if (!logFile.createNewFile()) {
                  LOGGER.debug(
                      "Partition log file has existed，filePath:{}", logFile.getAbsolutePath());
                }
                return new FileTimeIndexCacheWriter(logFile, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  public void close() throws IOException {
    for (Map<Long, FileTimeIndexCacheWriter> partitionWriterMap : writerMap.values()) {
      for (FileTimeIndexCacheWriter writer : partitionWriterMap.values()) {
        writer.close();
      }
    }
  }

  public void removeFileTimeIndexCache(int dataRegionId) {
    Map<Long, FileTimeIndexCacheWriter> partitionWriterMap = writerMap.get(dataRegionId);
    if (partitionWriterMap != null) {
      for (FileTimeIndexCacheWriter writer : partitionWriterMap.values()) {
        try {
          writer.close();
          deleteDirectoryAndEmptyParent(writer.getLogFile());
        } catch (IOException e) {
          LOGGER.warn("Meet error when close FileTimeIndexCache: {}", e.getMessage());
        }
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
